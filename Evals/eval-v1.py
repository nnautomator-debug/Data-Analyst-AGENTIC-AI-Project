import argparse
import importlib.util
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from openai import OpenAI

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EVAL_FILE = PROJECT_ROOT / "Data-Analytics/eval_dataset.txt"
DEFAULT_DATASET_PATH = PROJECT_ROOT / "Data /SuperStoreOrders.csv"
DEFAULT_OUTPUT = PROJECT_ROOT / "Evals/eval-v1-results.json"
NVIDIA_BASE_URL = "https://integrate.api.nvidia.com/v1"
JUDGE_MODEL = "nvidia/nemotron-3-ultra-550b-a55b"
SANDBOX_DATASET = "/home/user/SuperStoreOrders.csv"
MAX_JUDGE_CHARS = 24000

load_dotenv(PROJECT_ROOT / ".env")

UNSAFE_PATTERNS = [
    ("ipython_shell_escape", re.compile(r"(?m)^\s*!(?!=)")),
    ("ipython_shell_magic", re.compile(r"(?m)^\s*%%(?:bash|sh|shell|script)\b")),
    ("ipython_system_magic", re.compile(r"(?m)^\s*%(?:system|sx|bash|sh|shell)\b")),
    ("os_process", re.compile(r"\bos\.(?:system|popen|spawn\w*|exec\w*)\s*\(")),
    ("subprocess", re.compile(r"\bsubprocess\.\w+\s*\(")),
    ("pty", re.compile(r"\bpty\.spawn\s*\(")),
    ("shell_true", re.compile(r"\bshell\s*=\s*True\b")),
    ("shell_binary", re.compile(r"\b(?:bash|zsh|fish|powershell|cmd\.exe)\b")),
    ("network_shell", re.compile(r"\b(?:curl|wget)\b")),
    ("rm_rf", re.compile(r"\brm\s+-rf\b")),
    ("sudo", re.compile(r"\bsudo\b")),
    ("shutil_rmtree", re.compile(r"\bshutil\.rmtree\s*\(")),
    ("network_module", re.compile(r"\b(?:requests|httpx|urllib)\.\w+\s*\(")),
]

JUDGE_SYSTEM_PROMPT = """
You are a strict evaluator for an agentic data analytics system.
You compare the agent's plan and execution output with the expected plan and expected result.
The agent is allowed to return computed values, tables, rankings, and charts instead of a prose answer.
PASS only when the agent plan covers the required work and the execution output answers the user query with the correct scope, filters, grouping, aggregation, and values.
FAIL when required steps are missing, the result is wrong, the values differ materially, the wrong segment is analyzed, or the requested chart is missing.
Use the expected result as the source of truth.
Return only valid JSON with exactly two keys: verdict and reasoning.
verdict must be PASSED or FAILED.
reasoning must be one concise sentence.
"""

JUDGE_USER_TEMPLATE = """
USER QUERY:
{query}

EXPECTED PLAN:
{expected_plan}

EXPECTED RESULT:
{expected_result}

AGENT PLAN:
{agent_plan}

AGENT EXECUTION:
{agent_execution}

Return the JSON verdict now.
"""


def load_agent_module(name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module: {path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def load_eval_payload(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    cases = payload.get("cases")
    if not isinstance(cases, list) or not cases:
        raise RuntimeError(f"No cases found in {path}")
    return payload


def select_cases(
    cases: list[dict],
    case_id: str | None,
    start_id: str | None,
    limit: int | None,
) -> list[dict]:
    selected = cases
    if case_id:
        selected = [case for case in cases if case.get("id") == case_id]
        if not selected:
            raise RuntimeError(f"Case not found: {case_id}")
    else:
        if start_id:
            case_ids = [case.get("id") for case in cases]
            if start_id not in case_ids:
                raise RuntimeError(f"Start case not found: {start_id}")
            selected = cases[case_ids.index(start_id):]
        if limit is not None:
            selected = selected[:limit]
    return selected


def load_existing_results(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    cases = payload.get("cases") or []
    if not isinstance(cases, list):
        return []
    return cases


def get_judge_client() -> OpenAI:
    api_key = os.getenv("NVIDIA_API_KEY") or os.getenv("NVIDA_API_KEY")
    if not api_key:
        raise RuntimeError("NVIDIA_API_KEY is missing from the environment.")
    return OpenAI(base_url=NVIDIA_BASE_URL, api_key=api_key, timeout=240)


def truncate_text(text: str, limit: int = MAX_JUDGE_CHARS) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + "\n...[truncated]"


def format_execution(exec_result: dict) -> str:
    parts = [f"EXECUTION STATUS: {exec_result.get('execution_status', 'unknown')}"]

    execution_error = str(exec_result.get("error") or "").strip()
    if execution_error:
        parts.append(f"EXECUTION ERROR:\n{execution_error}")

    execution_log = exec_result.get("execution_log") or {}
    if not execution_log:
        parts.append("NO RUN_PYTHON COMMANDS RECORDED")
    else:
        for command_name, entry in execution_log.items():
            images = entry.get("images") or []
            parts.append(
                "\n".join(
                    [
                        f"{command_name}",
                        "CODE:",
                        str(entry.get("code") or ""),
                        "OUTPUT:",
                        str(entry.get("output") or "(no output)"),
                        "ERROR:",
                        str(entry.get("error") or "(none)"),
                        f"IMAGES: {len(images)}",
                    ]
                )
            )

    blocked_attempts = exec_result.get("blocked_attempts") or []
    if blocked_attempts:
        parts.append("BLOCKED ATTEMPTS:\n" + json.dumps(blocked_attempts, indent=2, default=str))

    return truncate_text("\n\n".join(parts))


def check_tool_calls(exec_result: dict) -> tuple[bool, str]:
    blocked_attempts = exec_result.get("blocked_attempts") or []
    if blocked_attempts:
        return False, f"Agent attempted {len(blocked_attempts)} blocked unsafe operation(s)."

    execution_log = exec_result.get("execution_log") or {}
    if not execution_log:
        return False, "Agent did not record any run_python calls."

    issues = []
    dataset_path_seen = False

    for command_name, entry in execution_log.items():
        code = entry.get("code")
        if not isinstance(code, str) or not code.strip():
            issues.append(f"{command_name}: empty code")
            continue

        try:
            compile(code, command_name, "exec")
        except SyntaxError as exc:
            issues.append(f"{command_name}: syntax error: {exc.msg} at line {exc.lineno}")

        for label, pattern in UNSAFE_PATTERNS:
            match = pattern.search(code)
            if match:
                issues.append(f"{command_name}: unsafe pattern {label}: {match.group(0)!r}")

        if SANDBOX_DATASET in code:
            dataset_path_seen = True

    if not dataset_path_seen:
        issues.append("No run_python call referenced the sandbox dataset path.")

    if issues:
        return False, "; ".join(issues[:10])

    return True, f"{len(execution_log)} run_python call(s) passed syntax, dataset-path, and safety checks."


def parse_judge_response(text: str) -> tuple[str, str]:
    cleaned = (text or "").strip()
    if not cleaned:
        return "FAILED", "Judge returned an empty response."

    candidates = [cleaned]
    json_match = re.search(r"\{.*?\}", cleaned, re.S)
    if json_match:
        candidates.insert(0, json_match.group(0))

    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue

        verdict = str(parsed.get("verdict", "")).strip().upper()
        reasoning = str(parsed.get("reasoning", "")).strip()
        if verdict in {"PASSED", "FAILED"}:
            return verdict, reasoning or "Judge returned no reasoning."

    verdict_matches = list(re.finditer(r"\b(PASSED|FAILED)\b", cleaned, re.I))
    if not verdict_matches:
        return "FAILED", "Judge did not return PASSED or FAILED."

    verdict = verdict_matches[-1].group(1).upper()
    lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
    reasoning = lines[-1] if lines else "Judge returned no reasoning."
    return verdict, reasoning


def judge_case(
    client: OpenAI,
    case: dict,
    agent_plan: list[dict],
    exec_result: dict,
) -> dict:
    expected_plan = json.dumps(case.get("expected_plan", []), indent=2, default=str)
    expected_result = json.dumps(case.get("expected_result", {}), indent=2, default=str)
    agent_plan_text = json.dumps(agent_plan, indent=2, default=str)
    agent_execution = format_execution(exec_result)

    prompt = JUDGE_USER_TEMPLATE.format(
        query=case.get("query", ""),
        expected_plan=truncate_text(expected_plan),
        expected_result=truncate_text(expected_result),
        agent_plan=truncate_text(agent_plan_text),
        agent_execution=agent_execution,
    )

    last_error = None
    for attempt in range(3):
        try:
            response = client.chat.completions.create(
                model=JUDGE_MODEL,
                messages=[
                    {"role": "system", "content": JUDGE_SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                temperature=0,
                max_tokens=800,
            )
            content = response.choices[0].message.content or ""
            verdict, reasoning = parse_judge_response(content)
            return {"verdict": verdict, "reasoning": reasoning}
        except Exception as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(2 ** attempt)

    raise last_error


def build_agent_tool_calls(exec_result: dict) -> list[dict]:
    execution_log = exec_result.get("execution_log") or {}
    tool_calls = []
    for command_name, entry in execution_log.items():
        tool_calls.append(
            {
                "name": "run_python",
                "command": command_name,
                "code": entry.get("code") or "",
                "output": entry.get("output") or "",
                "error": entry.get("error"),
                "image_count": len(entry.get("images") or []),
            }
        )
    return tool_calls


def run_case(
    case: dict,
    plan_module: Any,
    exec_module: Any,
    dataset_path: Path,
    dataset_profile: str,
    judge_client: OpenAI,
) -> dict:
    started = time.perf_counter()
    planning_result = plan_module.planning_node(
        {
            "dataset_path": str(dataset_path),
            "user_query": case["query"],
            "dataset_profile": dataset_profile,
        }
    )
    agent_plan = planning_result.get("plan") or []
    planning_error = str(planning_result.get("error") or "")

    if planning_error or not agent_plan:
        exec_result = {
            "execution_status": "error",
            "error": planning_error or "Planner returned no plan.",
            "execution_log": {},
            "images": [],
            "blocked_attempts": [],
        }
    else:
        exec_result = exec_module.exec_agent_node(
            {
                "dataset_path": str(dataset_path),
                "user_query": case["query"],
                "dataset_profile": dataset_profile,
                "plan": agent_plan,
            }
        )

    tool_passed, tool_reason = check_tool_calls(exec_result)
    try:
        judge = judge_case(judge_client, case, agent_plan, exec_result)
    except Exception as exc:
        judge = {"verdict": "FAILED", "reasoning": f"Judge error: {exc}"}
    passed = bool(tool_passed and judge["verdict"] == "PASSED")

    return {
        "id": case.get("id"),
        "category": case.get("category"),
        "difficulty": case.get("difficulty"),
        "query": case.get("query"),
        "expected_plan": case.get("expected_plan", []),
        "expected_result": case.get("expected_result", {}),
        "agent_plan": agent_plan,
        "planning_error": planning_error,
        "execution_status": exec_result.get("execution_status"),
        "execution_error": exec_result.get("error") or "",
        "agent_tool_calls": build_agent_tool_calls(exec_result),
        "image_count": len(exec_result.get("images") or []),
        "blocked_attempts": exec_result.get("blocked_attempts") or [],
        "tool_call_check": {"passed": tool_passed, "reason": tool_reason},
        "judge": judge,
        "passed": passed,
        "duration_seconds": round(time.perf_counter() - started, 2),
    }


def compute_metrics(results: list[dict]) -> dict:
    total = len(results)
    if total == 0:
        return {
            "total_accuracy": 0.0,
            "tool_call_accuracy": 0.0,
            "llm_judge_accuracy": 0.0,
            "full_passed": 0,
            "tool_passed": 0,
            "judge_passed": 0,
            "total_cases": 0,
        }

    full_passed = sum(1 for result in results if result["passed"])
    tool_passed = sum(1 for result in results if result["tool_call_check"]["passed"])
    judge_passed = sum(1 for result in results if result["judge"]["verdict"] == "PASSED")

    return {
        "total_accuracy": round(full_passed / total, 4),
        "tool_call_accuracy": round(tool_passed / total, 4),
        "llm_judge_accuracy": round(judge_passed / total, 4),
        "full_passed": full_passed,
        "tool_passed": tool_passed,
        "judge_passed": judge_passed,
        "total_cases": total,
    }


def write_output(
    output_path: Path,
    eval_path: Path,
    dataset_path: Path,
    results: list[dict],
    complete: bool,
) -> dict:
    metrics = compute_metrics(results)
    output = {
        "name": "eval-v1",
        "eval_file": str(eval_path),
        "dataset": str(dataset_path),
        "judge_model": JUDGE_MODEL,
        "profile_cached": True,
        "complete": complete,
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "case_count": len(results),
        "metrics": metrics,
        "cases": results,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--eval-file", default=str(DEFAULT_EVAL_FILE))
    parser.add_argument("--dataset", default=str(DEFAULT_DATASET_PATH))
    parser.add_argument("--case-id", default=None)
    parser.add_argument("--start-id", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = parser.parse_args()

    eval_path = Path(args.eval_file).expanduser().resolve()
    dataset_path = Path(args.dataset).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()

    payload = load_eval_payload(eval_path)
    selected_cases = select_cases(payload["cases"], args.case_id, args.start_id, args.limit)
    existing_results = load_existing_results(output_path) if args.resume else []
    existing_ids = {result.get("id") for result in existing_results}
    selected_cases = [case for case in selected_cases if case.get("id") not in existing_ids]
    if not selected_cases:
        raise RuntimeError("No cases left to run.")

    plan_module = load_agent_module("eval_plan_agent_v1", PROJECT_ROOT / "Agentic Loop /plan-agent.py")
    exec_module = load_agent_module("eval_exec_agent_v1", PROJECT_ROOT / "Agentic Loop /exec-agent.py")
    judge_client = get_judge_client()

    print(f"Profiling dataset once: {dataset_path}", flush=True)
    profile_started = time.perf_counter()
    dataset_profile = plan_module.profile_dataset(dataset_path)
    print(f"Profile ready in {time.perf_counter() - profile_started:.2f}s", flush=True)
    if existing_results:
        print(f"Resuming with {len(existing_results)} existing cases; running {len(selected_cases)} new cases.", flush=True)

    all_case_ids = [case.get("id") for case in payload["cases"]]
    results = existing_results
    for case in selected_cases:
        case_index = all_case_ids.index(case.get("id")) + 1
        print(f"[{case_index}/{len(all_case_ids)}] {case['id']}: {case['query']}", flush=True)
        result = run_case(
            case,
            plan_module,
            exec_module,
            dataset_path,
            dataset_profile,
            judge_client,
        )
        results.append(result)
        write_output(output_path, eval_path, dataset_path, results, complete=False)
        with open(PROJECT_ROOT / "Evals/eval-v1-run.log", "a", encoding="utf-8") as log_file:
            log_file.write(
                f"[{case_index}/{len(all_case_ids)}] {case['id']}: {case['query']}\n"
                f"  tool={result['tool_call_check']['passed']} "
                f"judge={result['judge']['verdict']} "
                f"passed={result['passed']} "
                f"time={result['duration_seconds']}s\n"
            )
            log_file.flush()
        print(
            f"  tool={result['tool_call_check']['passed']} "
            f"judge={result['judge']['verdict']} "
            f"passed={result['passed']} "
            f"time={result['duration_seconds']}s",
            flush=True,
        )

    metrics = write_output(output_path, eval_path, dataset_path, results, complete=True)

    print("\nRESULTS", flush=True)
    print(json.dumps(metrics, indent=2), flush=True)
    print(f"Wrote {output_path}", flush=True)


if __name__ == "__main__":
    main()
