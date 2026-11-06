import argparse
import importlib.util
import json
import os
import re
import sys
import time
import uuid
from pathlib import Path
from typing import Any, TypedDict

from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate
from langchain_groq import ChatGroq
from langgraph.graph import END, START, StateGraph

PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")

DEFAULT_DATASET_PATH = PROJECT_ROOT / "Data /SuperStoreOrders.csv"
DEFAULT_GROQ_MODEL = "openai/gpt-oss-120b"
DEFAULT_MAX_REPAIR_ATTEMPTS = 2
MAX_LOG_CHARS = 16000
MAX_TRANSCRIPT_CHARS = 24000

ERROR_PATTERNS = [
    ("traceback", re.compile(r"Traceback \(most recent call last\):")),
    (
        "python_error",
        re.compile(
            r"(?m)^(?:SyntaxError|NameError|TypeError|ValueError|KeyError|"
            r"AttributeError|ImportError|ModuleNotFoundError|IndexError|"
            r"ZeroDivisionError|FileNotFoundError|AssertionError|RuntimeError):"
        ),
    ),
    ("error_prefix", re.compile(r"(?m)^(?:ERROR|Error):\s")),
    ("file_line", re.compile(r'File "[^"]*", line \d+')),
]


class AgentState(TypedDict, total=False):
    job_id: str
    dataset_path: str
    user_query: str
    dataset_profile: str
    plan: list[dict]

    validation: dict
    max_scripts: int
    sandbox_path: str
    execution_log: dict
    images: list[str]
    blocked_attempts: list[dict]
    execution_status: str

    sandbox_failures: list[dict]
    active_failure: dict
    code_check: dict
    repair_context: dict
    repair_attempts: dict
    repair_log: list[dict]
    max_repair_attempts: int
    repair_exhausted: bool

    final_answer: str
    artifacts: list[dict]

    stage: str
    status: str
    error: str
    errors: list[str]
    timings: dict
    agent_steps: list[dict]
    created_at: str
    updated_at: str


def load_agent_module(name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module: {path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


PLAN_AGENT = load_agent_module("main_plan_agent", PROJECT_ROOT / "Agentic Loop /plan-agent.py")
EXEC_AGENT = load_agent_module("main_exec_agent", PROJECT_ROOT / "Agentic Loop /exec-agent.py")
CHECK_AGENT = load_agent_module("main_code_check_agent", PROJECT_ROOT / "Agentic Loop /code-check-agent.py")


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _truncate(value: Any, limit: int) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[:limit] + "\n...[truncated]"


def _node_failure(state: AgentState, node: str, exc: Exception | str) -> dict:
    message = f"{node} failed: {exc}"
    return {
        "stage": node,
        "status": "failed",
        "error": message,
        "errors": [*state.get("errors", []), message],
        "updated_at": _now(),
    }


_KAFKA_EVENTS_MODULE = None


def _publish_run_event(state: AgentState) -> None:
    if not os.getenv("KAFKA_ENABLED", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return

    global _KAFKA_EVENTS_MODULE
    try:
        if _KAFKA_EVENTS_MODULE is None:
            _KAFKA_EVENTS_MODULE = load_agent_module(
                "main_kafka_events",
                PROJECT_ROOT / "Kafka -Evant Bus/kafka-events.py",
            )
        _KAFKA_EVENTS_MODULE.publish_run_completed(state)
    except Exception as exc:
        print(f"Kafka event publish failed: {exc}", file=sys.stderr)


def _timings(state: AgentState) -> dict:
    return dict(state.get("timings") or {})


def _strip_images(log: dict) -> dict:
    cleaned = {}
    for command_name, entry in (log or {}).items():
        if not isinstance(entry, dict):
            cleaned[command_name] = entry
            continue
        item = dict(entry)
        images = item.pop("images", [])
        item["image_count"] = len(images)
        cleaned[command_name] = item
    return cleaned


def _step_state(state: AgentState) -> dict:
    snapshot = dict(state)
    images = snapshot.pop("images", [])
    snapshot["image_count"] = len(images)
    snapshot["execution_log"] = _strip_images(snapshot.get("execution_log", {}))
    repair_log = []
    for entry in snapshot.get("repair_log", []):
        if not isinstance(entry, dict):
            continue
        item = dict(entry)
        item["execution_log"] = _strip_images(item.get("execution_log", {}))
        repair_log.append(item)
    snapshot["repair_log"] = repair_log
    return snapshot


def profile_node(state: AgentState) -> dict:
    started = time.perf_counter()
    dataset_path = str(state.get("dataset_path") or DEFAULT_DATASET_PATH)
    try:
        path = Path(dataset_path).expanduser().resolve()
        if not path.is_file():
            raise FileNotFoundError(f"Dataset not found: {path}")

        dataset_profile = PLAN_AGENT.profile_dataset(path)
        timings = _timings(state)
        timings["profile"] = round(time.perf_counter() - started, 2)
        return {
            "dataset_path": str(path),
            "dataset_profile": dataset_profile,
            "stage": "profile",
            "status": "running",
            "error": "",
            "timings": timings,
            "updated_at": _now(),
        }
    except Exception as exc:
        return _node_failure(state, "profile", exc)


def plan_node(state: AgentState) -> dict:
    started = time.perf_counter()
    try:
        user_query = str(state.get("user_query") or "").strip()
        if not user_query:
            raise ValueError("A non-empty user_query is required.")

        result = PLAN_AGENT.planning_node(
            {
                "dataset_path": state["dataset_path"],
                "user_query": user_query,
                "dataset_profile": state.get("dataset_profile", ""),
            }
        )
        if result.get("error"):
            raise RuntimeError(result["error"])

        plan = result.get("plan") or []
        if not plan:
            raise RuntimeError("Planner returned no steps.")

        timings = _timings(state)
        timings["plan"] = round(time.perf_counter() - started, 2)
        return {
            "dataset_profile": result.get("dataset_profile") or state.get("dataset_profile", ""),
            "plan": plan,
            "stage": "plan",
            "status": "running",
            "error": "",
            "timings": timings,
            "updated_at": _now(),
        }
    except Exception as exc:
        return _node_failure(state, "plan", exc)


def exec_node(state: AgentState) -> dict:
    started = time.perf_counter()
    try:
        result = EXEC_AGENT.exec_agent_node(
            {
                "dataset_path": state["dataset_path"],
                "user_query": state.get("user_query", ""),
                "dataset_profile": state.get("dataset_profile", ""),
                "plan": state.get("plan", []),
            }
        )
        timings = _timings(state)
        timings["exec"] = round(time.perf_counter() - started, 2)
        updates = {
            "validation": result.get("validation", {}),
            "max_scripts": result.get("max_scripts", 0),
            "sandbox_path": result.get("sandbox_path", ""),
            "execution_log": result.get("execution_log", {}),
            "images": result.get("images", []),
            "blocked_attempts": result.get("blocked_attempts", []),
            "execution_status": result.get("execution_status", "error"),
            "stage": "exec",
            "status": "running",
            "error": "",
            "timings": timings,
            "updated_at": _now(),
        }
        if result.get("execution_status") == "error":
            updates["error"] = result.get("error") or "Exec Agent failed."
        return updates
    except Exception as exc:
        return _node_failure(state, "exec", exc)


def extract_error_line(error_text: str) -> int:
    matches = re.findall(r'File ".*?", line (\d+)', str(error_text or ""))
    return int(matches[-1]) if matches else 0


def detect_sandbox_failures(execution_log: dict) -> list[dict]:
    failures: list[dict] = []
    for command_name, entry in (execution_log or {}).items():
        if not isinstance(entry, dict):
            continue

        error_text = str(entry.get("error") or "").strip()
        output = str(entry.get("output") or "")
        haystack = error_text or output
        matched = [label for label, pattern in ERROR_PATTERNS if pattern.search(haystack)]

        if not error_text and not matched:
            continue

        failures.append(
            {
                "command": command_name,
                "code": str(entry.get("code") or ""),
                "error": error_text or output,
                "output": output,
                "line_number": extract_error_line(haystack),
                "patterns": matched,
            }
        )
    return failures


def select_active_failure(
    failures: list[dict],
    repair_attempts: dict,
    max_repair_attempts: int,
) -> tuple[dict, bool]:
    for failure in failures:
        command = failure.get("command", "")
        attempts = int(repair_attempts.get(command, 0))
        if attempts < max_repair_attempts:
            return failure, False
    return {}, bool(failures)


def inspect_node(state: AgentState) -> dict:
    failures = detect_sandbox_failures(state.get("execution_log", {}))
    repair_attempts = dict(state.get("repair_attempts") or {})
    max_attempts = int(state.get("max_repair_attempts", DEFAULT_MAX_REPAIR_ATTEMPTS))
    active_failure, exhausted = select_active_failure(
        failures,
        repair_attempts,
        max_attempts,
    )

    stage = "inspect"
    status = "running"
    error = ""
    errors = list(state.get("errors") or [])

    if failures and exhausted:
        error = "Repair attempts exhausted for all failed commands."
        status = "failed"
        errors.append(error)

    return {
        "sandbox_failures": failures,
        "active_failure": active_failure,
        "repair_exhausted": exhausted,
        "stage": stage,
        "status": status,
        "error": error,
        "errors": errors,
        "updated_at": _now(),
    }


def code_check_node(state: AgentState) -> dict:
    started = time.perf_counter()
    failure = state.get("active_failure") or {}
    try:
        if not failure:
            raise RuntimeError("No active sandbox failure to check.")

        result = CHECK_AGENT.code_check_node(
            {
                "code": failure.get("code", ""),
                "sandbox_error": failure.get("error", ""),
                "stdout": failure.get("output", ""),
                "stderr": "",
                "user_query": state.get("user_query", ""),
                "dataset_profile": state.get("dataset_profile", ""),
                "plan": state.get("plan", []),
            }
        )
        timings = _timings(state)
        timings["code_check"] = round(time.perf_counter() - started, 2)

        if result.get("error"):
            raise RuntimeError(result["error"])

        if result.get("verdict") == "PASS":
            raise RuntimeError("Code checker returned PASS for a failed sandbox command.")

        return {
            "code_check": result,
            "stage": "code_check",
            "status": "running",
            "error": "",
            "timings": timings,
            "updated_at": _now(),
        }
    except Exception as exc:
        return _node_failure(state, "code_check", exc)


def build_repair_context(
    state: AgentState,
    failure: dict,
    code_check: dict,
    attempt: int,
) -> dict:
    transcript = []
    for command_name, entry in (state.get("execution_log") or {}).items():
        if not isinstance(entry, dict):
            continue
        transcript.append(
            {
                "command": command_name,
                "code": _truncate(entry.get("code", ""), 4000),
                "output": _truncate(entry.get("output", ""), 4000),
                "error": _truncate(entry.get("error", ""), 4000),
            }
        )

    return {
        "attempt": attempt,
        "failed_command": failure.get("command", ""),
        "failed_code": failure.get("code", ""),
        "sandbox_error": failure.get("error", ""),
        "error_line": failure.get("line_number", 0),
        "checker_cause": code_check.get("cause", ""),
        "checker_reasoning": code_check.get("reasoning", ""),
        "checker_fixes": code_check.get("fixes", []),
        "corrected_code": code_check.get("corrected_code", ""),
        "execution_transcript": transcript,
        "instructions": (
            "This repair run uses a fresh sandbox. Recreate the required prior "
            "computation from the original plan and execution transcript, apply "
            "the checker fixes by line number, rerun the failed command, and "
            "preserve the original user query scope."
        ),
    }


def _run_corrected_code(
    state: AgentState,
    failure: dict,
    corrected_code: str,
) -> dict:
    code = str(corrected_code or "").strip()
    if not code:
        raise ValueError("Corrected code is empty.")

    static_issues = CHECK_AGENT._static_checks(code)
    blocking = [
        issue
        for issue in static_issues
        if "syntax error" in issue or "tech-stack violation" in issue
    ]
    if blocking:
        error = "Corrected code failed static checks: " + "; ".join(blocking)
        return {
            "execution_log": {
                "command_1": {
                    "code": code,
                    "output": f"ERROR:\n{error}",
                    "error": error,
                    "images": [],
                }
            },
            "images": [],
            "blocked_attempts": [],
            "sandbox_path": "",
            "execution_status": "completed",
            "error": "",
        }

    failed_command = str(failure.get("command") or "")
    prior_codes = []
    for command_name, entry in (state.get("execution_log") or {}).items():
        if command_name == failed_command:
            break
        if not isinstance(entry, dict) or entry.get("error"):
            break
        prior_code = str(entry.get("code") or "").strip()
        if prior_code:
            prior_codes.append(prior_code)

    combined_code = "\n\n".join([*prior_codes, code])
    sandbox = None
    sandbox_path = ""
    execution_log: dict = {}
    images: list[str] = []

    try:
        sandbox, sandbox_path = EXEC_AGENT._prepare_sandbox(state["dataset_path"])
        run_python = EXEC_AGENT._build_run_python(sandbox, execution_log, images)
        run_python.invoke({"code": combined_code})
        return {
            "execution_log": execution_log,
            "images": images,
            "blocked_attempts": [],
            "sandbox_path": sandbox_path,
            "execution_status": "completed",
            "error": "",
        }
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        if not execution_log:
            execution_log["command_1"] = {
                "code": combined_code,
                "output": f"ERROR:\n{error}",
                "error": error,
                "images": [],
            }
        if sandbox is None:
            return {
                "execution_log": execution_log,
                "images": images,
                "blocked_attempts": [],
                "sandbox_path": sandbox_path,
                "execution_status": "error",
                "error": f"Corrected code execution failed: {exc}",
            }
        return {
            "execution_log": execution_log,
            "images": images,
            "blocked_attempts": [],
            "sandbox_path": sandbox_path,
            "execution_status": "completed",
            "error": "",
        }
    finally:
        if sandbox is not None:
            sandbox.kill()


def repair_node(state: AgentState) -> dict:
    started = time.perf_counter()
    failure = state.get("active_failure") or {}
    code_check = state.get("code_check") or {}
    command = str(failure.get("command") or "")
    attempts = dict(state.get("repair_attempts") or {})
    attempt = int(attempts.get(command, 0)) + 1
    attempts[command] = attempt

    try:
        if not failure:
            raise RuntimeError("No active sandbox failure to repair.")
        if not code_check:
            raise RuntimeError("No checker result is available for repair.")

        repair_context = build_repair_context(state, failure, code_check, attempt)
        corrected_code = str(code_check.get("corrected_code") or "").strip()

        if corrected_code:
            result = _run_corrected_code(state, failure, corrected_code)
        else:
            result = EXEC_AGENT.exec_agent_node(
                {
                    "dataset_path": state["dataset_path"],
                    "user_query": state.get("user_query", ""),
                    "dataset_profile": state.get("dataset_profile", ""),
                    "plan": state.get("plan", []),
                    "validation": state.get("validation", {}),
                    "max_scripts": state.get("max_scripts", 0),
                    "repair_context": repair_context,
                }
            )

        repair_entry = {
            "command": command,
            "attempt": attempt,
            "checker_cause": code_check.get("cause", ""),
            "checker_reasoning": code_check.get("reasoning", ""),
            "checker_fixes": code_check.get("fixes", []),
            "corrected_code": corrected_code,
            "repair_context": repair_context,
            "execution_status": result.get("execution_status", "error"),
            "error": result.get("error", ""),
            "execution_log": result.get("execution_log", {}),
        }

        timings = _timings(state)
        timings["repair"] = round(time.perf_counter() - started, 2)

        updates = {
            "validation": result.get("validation", state.get("validation", {})),
            "max_scripts": result.get("max_scripts", state.get("max_scripts", 0)),
            "sandbox_path": result.get("sandbox_path", ""),
            "execution_log": result.get("execution_log", {}),
            "images": result.get("images", []),
            "blocked_attempts": result.get("blocked_attempts", []),
            "execution_status": result.get("execution_status", "error"),
            "repair_attempts": attempts,
            "repair_log": [*(state.get("repair_log") or []), repair_entry],
            "active_failure": {},
            "code_check": {},
            "repair_context": repair_context,
            "stage": "repair",
            "status": "running",
            "error": "",
            "timings": timings,
            "updated_at": _now(),
        }

        if result.get("execution_status") == "error":
            updates["error"] = result.get("error") or "Repair run failed."
        return updates
    except Exception as exc:
        return _node_failure(state, "repair", exc)


def _format_execution(execution_log: dict) -> str:
    parts = []
    for command_name, entry in (execution_log or {}).items():
        if not isinstance(entry, dict):
            continue
        parts.append(
            "\n".join(
                [
                    f"{command_name}",
                    "CODE:",
                    _truncate(entry.get("code", ""), 4000),
                    "OUTPUT:",
                    _truncate(entry.get("output", ""), 4000),
                    "ERROR:",
                    _truncate(entry.get("error", ""), 2000) or "(none)",
                ]
            )
        )
    return _truncate("\n\n".join(parts), MAX_TRANSCRIPT_CHARS)


def _get_groq_llm() -> ChatGroq:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY is missing from the environment.")

    return ChatGroq(
        model=os.getenv("GROQ_MODEL", DEFAULT_GROQ_MODEL),
        api_key=api_key,
        temperature=0,
        timeout=120,
        max_retries=2,
    )


def synthesis_node(state: AgentState) -> dict:
    started = time.perf_counter()
    try:
        execution_log = state.get("execution_log") or {}
        repair_log = state.get("repair_log") or []
        prompt = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    "You are the Synthesis Agent in a data analytics system. "
                    "Answer the user's request using only values and tables present in the "
                    "execution output. Do not invent numbers or conclusions. If execution "
                    "failed or data is missing, state that clearly and concisely.",
                ),
                (
                    "human",
                    "USER QUERY:\n{user_query}\n\n"
                    "PLANNER STEPS:\n{plan}\n\n"
                    "EXECUTION OUTPUT:\n{execution}\n\n"
                    "REPAIR HISTORY:\n{repair_history}\n\n"
                    "IMAGE COUNT: {image_count}\n\n"
                    "Return a clear final answer.",
                ),
            ]
        )
        messages = prompt.format_messages(
            user_query=state.get("user_query", ""),
            plan=json.dumps(state.get("plan", []), indent=2, default=str),
            execution=_format_execution(execution_log),
            repair_history=_truncate(
                json.dumps(repair_log, indent=2, default=str),
                MAX_LOG_CHARS,
            ),
            image_count=len(state.get("images") or []),
        )
        response = _get_groq_llm().invoke(messages)
        content = response.content
        if isinstance(content, list):
            content = "\n".join(
                str(part.get("text", "")) if isinstance(part, dict) else str(part)
                for part in content
            )

        timings = _timings(state)
        timings["synthesis"] = round(time.perf_counter() - started, 2)
        return {
            "final_answer": str(content).strip(),
            "artifacts": [],
            "stage": "synthesis",
            "status": "completed",
            "error": "",
            "timings": timings,
            "updated_at": _now(),
        }
    except Exception as exc:
        return _node_failure(state, "synthesis", exc)


def failure_node(state: AgentState) -> dict:
    message = state.get("error") or "Workflow failed."
    errors = [*(state.get("errors") or [])]
    if message not in errors:
        errors.append(message)
    return {
        "stage": "failure",
        "status": "failed",
        "error": message,
        "errors": errors,
        "updated_at": _now(),
    }


def _route_after_profile(state: AgentState) -> str:
    return "failure" if state.get("error") else "plan"


def _route_after_plan(state: AgentState) -> str:
    return "failure" if state.get("error") else "exec"


def _route_after_exec(state: AgentState) -> str:
    return "failure" if state.get("error") else "inspect"


def _route_after_inspect(state: AgentState) -> str:
    if state.get("error"):
        return "failure"
    if not state.get("sandbox_failures"):
        return "synthesis"
    if state.get("repair_exhausted"):
        return "failure"
    return "code_check"


def _route_after_code_check(state: AgentState) -> str:
    return "failure" if state.get("error") else "repair"


def _route_after_repair(state: AgentState) -> str:
    return "failure" if state.get("error") else "inspect"


def build_graph():
    workflow = StateGraph(AgentState)
    workflow.add_node("profile", profile_node)
    workflow.add_node("plan", plan_node)
    workflow.add_node("exec", exec_node)
    workflow.add_node("inspect", inspect_node)
    workflow.add_node("code_check", code_check_node)
    workflow.add_node("repair", repair_node)
    workflow.add_node("synthesis", synthesis_node)
    workflow.add_node("failure", failure_node)

    workflow.add_edge(START, "profile")
    workflow.add_conditional_edges(
        "profile",
        _route_after_profile,
        {"plan": "plan", "failure": "failure"},
    )
    workflow.add_conditional_edges(
        "plan",
        _route_after_plan,
        {"exec": "exec", "failure": "failure"},
    )
    workflow.add_conditional_edges(
        "exec",
        _route_after_exec,
        {"inspect": "inspect", "failure": "failure"},
    )
    workflow.add_conditional_edges(
        "inspect",
        _route_after_inspect,
        {
            "synthesis": "synthesis",
            "code_check": "code_check",
            "failure": "failure",
        },
    )
    workflow.add_conditional_edges(
        "code_check",
        _route_after_code_check,
        {"repair": "repair", "failure": "failure"},
    )
    workflow.add_conditional_edges(
        "repair",
        _route_after_repair,
        {"inspect": "inspect", "failure": "failure"},
    )
    workflow.add_edge("synthesis", END)
    workflow.add_edge("failure", END)
    return workflow.compile()


def run_workflow(
    dataset_path: str | Path,
    user_query: str,
    job_id: str | None = None,
    max_repair_attempts: int = DEFAULT_MAX_REPAIR_ATTEMPTS,
) -> AgentState:
    started = time.perf_counter()
    timestamp = _now()
    state: AgentState = {
        "job_id": job_id or uuid.uuid4().hex,
        "dataset_path": str(Path(dataset_path).expanduser().resolve()),
        "user_query": user_query,
        "max_repair_attempts": max_repair_attempts,
        "repair_attempts": {},
        "repair_log": [],
        "errors": [],
        "timings": {},
        "stage": "queued",
        "status": "running",
        "error": "",
        "created_at": timestamp,
        "updated_at": timestamp,
    }

    steps: list[dict] = []
    result = state
    try:
        for snapshot in build_graph().stream(state, stream_mode="values"):
            result = dict(snapshot)
            steps.append(
                {
                    "step": len(steps),
                    "stage": result.get("stage", ""),
                    "status": result.get("status", ""),
                    "error": result.get("error", ""),
                    "state": _step_state(result),
                }
            )
        timings = dict(result.get("timings") or {})
        timings["total"] = round(time.perf_counter() - started, 2)
        result["timings"] = timings
        result["updated_at"] = _now()
        result["agent_steps"] = steps
        _publish_run_event(result)
        return result
    except Exception as exc:
        message = f"Workflow failed: {exc}"
        state.update(
            {
                "error": message,
                "errors": [*state.get("errors", []), message],
                "stage": "failure",
                "status": "failed",
                "timings": {
                    **state.get("timings", {}),
                    "total": round(time.perf_counter() - started, 2),
                },
                "updated_at": _now(),
                "agent_steps": steps,
            }
        )
        _publish_run_event(state)
        return state


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("query", nargs="?", default="What is the total sales?")
    parser.add_argument("--dataset", default=str(DEFAULT_DATASET_PATH))
    parser.add_argument("--job-id", default=None)
    parser.add_argument(
        "--max-repair-attempts",
        type=int,
        default=DEFAULT_MAX_REPAIR_ATTEMPTS,
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    result = run_workflow(
        dataset_path=args.dataset,
        user_query=args.query,
        job_id=args.job_id,
        max_repair_attempts=args.max_repair_attempts,
    )

    if args.json:
        print(json.dumps(result, indent=2, default=str))
        return

    if result.get("error"):
        print(f"FAILED: {result['error']}")
        return

    print(result.get("final_answer") or "No final answer was produced.")


if __name__ == "__main__":
    main()
