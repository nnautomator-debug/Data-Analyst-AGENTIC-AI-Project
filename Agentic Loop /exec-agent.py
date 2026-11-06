import json
import os
import re
from pathlib import Path
from typing import TypedDict

from dotenv import load_dotenv
from e2b_code_interpreter import Sandbox
from langchain.agents import create_agent
from langchain.agents.middleware import AgentMiddleware, ToolCallLimitMiddleware
from langchain_core.messages import HumanMessage, ToolMessage
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

NVIDIA_BASE_URL = "https://integrate.api.nvidia.com/v1"
DEFAULT_NVIDIA_MODEL = "nvidia/nemotron-3.5-super-vl-120b-a12b"
MAX_TOOL_OUTPUT_CHARS = 12000


class AgentState(TypedDict, total=False):
    dataset_path: str
    user_query: str
    dataset_profile: str
    plan: list[dict]
    repair_context: dict
    validation: dict
    max_scripts: int
    sandbox_path: str
    execution_log: dict
    images: list[str]
    blocked_attempts: list[dict]
    execution_status: str
    error: str


class ValidationScript(BaseModel):
    script: int = Field(ge=1)
    purpose: str = Field(min_length=10)
    steps_covered: list[str] = Field(default_factory=list)


class ValidationResult(BaseModel):
    valid: bool
    script_count: int = Field(default=0, ge=0)
    scripts: list[ValidationScript] = Field(default_factory=list)
    reason: str = ""


VALIDATOR_SYSTEM_PROMPT = """
You are the Plan Validator in a multi-agent data analytics system.
You do not write code and you do not execute anything.
Validate the planner steps against the user request and the dataset profile.
Estimate how many Python scripts are required and state exactly what each script must do.
Rules:
- Only use columns that exist in the dataset profile.
- Group related planner steps into one script when possible.
- Script purposes must be executable with pandas, numpy, matplotlib, or seaborn.
- Set valid to false when required columns or operations are impossible.
- script_count must equal the number of entries in scripts.
- Return only valid JSON with no markdown fences or commentary.

{format_instructions}
"""

VALIDATOR_USER_PROMPT = """
User request:
{user_query}

Planner steps:
{plan}

Dataset profile:
{dataset_profile}
"""

EXEC_SYSTEM_PROMPT = """
You are the Exec Agent in a multi-agent data analytics system.
Your only job is to convert the planner steps into Python code and execute that code with the run_python tool.
You must not write business insights, recommendations, summaries, or final answers.
You must use the exact column names and categorical values provided in the dataset profile.
Use only Python data-analysis libraries such as pandas, numpy, matplotlib, and seaborn.
Never use bash, shell, terminal commands, subprocess, os.system, or os.popen.
Use the prepared sandbox dataset path instead of uploading data again.
Write self-contained code for the current step or group of steps.
Before calling run_python, review the Python code for valid syntax, correct indentation, balanced parentheses, defined variables, and all required imports. Send only syntactically valid code to the tool.
For every CSV read, inspect object-typed columns that may contain numeric values. If comma-formatted numbers are possible, use pd.read_csv with thousands="," or remove commas before pd.to_numeric.
Never silently accept data loss from errors="coerce". If a numeric conversion creates NaNs in a column that had no missing values, fix the parsing logic instead of continuing with the loss.
NON-NEGOTIABLE: Read this dataset only with pd.read_csv(path, thousands=",") and never let comma-formatted sales values reach pd.to_numeric, where errors="coerce" turns them into NaN and silently drops sales.
Inspect stdout, stderr, and tracebacks from each execution and correct the code when needed.
When parsing date-like object columns, use pd.to_datetime with format="mixed", dayfirst=True, and errors="coerce" unless the profile proves a single format.
Use .copy() after filtering, and create derived date columns before groupby because aggregated frames do not preserve original datetime columns.
Use plt.show() so matplotlib figures are captured as PNG outputs.
If repair context is provided, treat it as a fresh-sandbox repair run: recreate the required prior computation from the original plan and execution transcript, apply the checker fixes by line number, rerun the failed command, and preserve the original user query scope.
Stop when the required data and images have been produced or when no tool calls remain.
"""


def _get_llm() -> ChatOpenAI:
    api_key = os.getenv("NVIDIA_API_KEY") or os.getenv("NVIDA_API_KEY")
    if not api_key:
        raise RuntimeError("NVIDIA_API_KEY is missing from the environment.")

    return ChatOpenAI(
        model=os.getenv("NVIDIA_MODEL", DEFAULT_NVIDIA_MODEL),
        base_url=os.getenv("NVIDIA_BASE_URL", NVIDIA_BASE_URL),
        api_key=api_key,
        temperature=0,
        timeout=120,
        max_retries=2,
    )


def _joined_logs(execution) -> tuple[str, str]:
    stdout = "\n".join(execution.logs.stdout).strip() if execution.logs else ""
    stderr = "\n".join(execution.logs.stderr).strip() if execution.logs else ""
    return stdout, stderr


def _execution_error(execution) -> str | None:
    if not execution.error:
        return None
    return (
        f"{execution.error.name}: {execution.error.value}\n"
        f"{execution.error.traceback}"
    )


def _prepare_sandbox(dataset_path: str | Path) -> tuple[Sandbox, str]:
    path = Path(dataset_path).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Dataset not found: {path}")

    sandbox = Sandbox.create()
    sandbox_path = f"/home/user/{path.name}"

    try:
        sandbox.files.write(
            sandbox_path,
            path.read_bytes(),
            request_timeout=120,
        )
        return sandbox, sandbox_path
    except Exception:
        sandbox.kill()
        raise


def _validate_plan(
    llm: ChatOpenAI,
    user_query: str,
    dataset_profile: str,
    plan: list[dict],
) -> ValidationResult:
    parser = JsonOutputParser(pydantic_object=ValidationResult)
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", VALIDATOR_SYSTEM_PROMPT),
            ("human", VALIDATOR_USER_PROMPT),
        ]
    ).partial(format_instructions=parser.get_format_instructions())

    result = (prompt | llm | parser).invoke(
        {
            "user_query": user_query,
            "plan": json.dumps(plan, indent=2, default=str),
            "dataset_profile": dataset_profile,
        }
    )
    validation = ValidationResult.model_validate(result)

    if validation.valid:
        if not validation.scripts:
            validation.valid = False
            validation.reason = "Validator returned no scripts."
        else:
            validation.script_count = len(validation.scripts)

    return validation


def _truncate(text: str) -> str:
    if len(text) <= MAX_TOOL_OUTPUT_CHARS:
        return text
    return text[:MAX_TOOL_OUTPUT_CHARS] + "\n...[truncated]"


def _build_run_python(
    sandbox: Sandbox,
    execution_log: dict,
    images: list[str],
):
    @tool
    def run_python(code: str) -> str:
        """Execute Python code inside the shared E2B sandbox."""
        command_number = len(execution_log) + 1
        command_key = f"command_{command_number}"

        try:
            execution = sandbox.run_code(code, timeout=120)
            stdout, stderr = _joined_logs(execution)
            error = _execution_error(execution)
            result_texts = [
                result.text
                for result in (execution.results or [])
                if getattr(result, "text", None)
            ]
            command_images = [
                result.png
                for result in (execution.results or [])
                if getattr(result, "png", None)
            ]

            output_parts = []
            if stdout:
                output_parts.append(f"STDOUT:\n{stdout}")
            if result_texts:
                output_parts.append("RESULT TEXT:\n" + "\n".join(result_texts))
            if stderr:
                output_parts.append(f"STDERR:\n{stderr}")
            if error:
                output_parts.append(f"ERROR:\n{error}")

            output = "\n\n".join(output_parts).strip()
            execution_log[command_key] = {
                "code": code,
                "output": output,
                "error": error,
                "images": command_images,
            }
            images.extend(command_images)

            visible_output = _truncate(output) or "(no output)"
            message = (
                f"{command_key} executed.\n\n"
                f"{visible_output}\n\n"
                f"PNG images captured: {len(command_images)}"
            )
            return message
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            execution_log[command_key] = {
                "code": code,
                "output": "",
                "error": error,
                "images": [],
            }
            return f"{command_key} failed before execution.\n\nERROR:\n{error}"

    return run_python


class CodeSafetyMiddleware(AgentMiddleware):
    BLOCKED_PATTERNS = [
        (re.compile(r"(?m)^\s*!(?!=)"), "IPython shell escape"),
        (
            re.compile(r"(?m)^\s*%%(?:bash|sh|shell|script)\b"),
            "IPython shell magic",
        ),
        (
            re.compile(r"(?m)^\s*%(?:system|sx|bash|sh|shell)\b"),
            "IPython system magic",
        ),
        (
            re.compile(r"\bos\.(?:system|popen|spawn\w*|exec\w*)\s*\("),
            "OS process execution",
        ),
        (re.compile(r"\bsubprocess\.\w+\s*\("), "subprocess execution"),
        (re.compile(r"\bpty\.spawn\s*\("), "pty execution"),
        (re.compile(r"\bshell\s*=\s*True\b"), "shell=True"),
        (
            re.compile(r"\b(?:bash|zsh|fish|powershell|cmd\.exe)\b"),
            "shell binary",
        ),
        (re.compile(r"\b(?:curl|wget)\b"), "network shell command"),
        (re.compile(r"\brm\s+-rf\b"), "destructive shell command"),
        (re.compile(r"\bsudo\b"), "privileged shell command"),
    ]

    def __init__(self, blocked_attempts: list[dict]):
        self.blocked_attempts = blocked_attempts

    def wrap_tool_call(self, request, handler):
        if request.tool_call.get("name") != "run_python":
            return handler(request)

        code = str(request.tool_call.get("args", {}).get("code", ""))
        for pattern, label in self.BLOCKED_PATTERNS:
            match = pattern.search(code)
            if not match:
                continue

            self.blocked_attempts.append(
                {
                    "pattern": label,
                    "match": match.group(0),
                    "code": code,
                }
            )
            return ToolMessage(
                content=(
                    "Blocked unsafe code before sandbox execution. "
                    f"Matched {label}: {match.group(0)!r}. "
                    "Rewrite the step using pandas, numpy, matplotlib, or seaborn only."
                ),
                tool_call_id=request.tool_call.get("id") or "",
                name="run_python",
                status="error",
            )

        return handler(request)


def _execution_task(
    state: AgentState,
    validation: ValidationResult,
    sandbox_path: str,
    max_scripts: int,
) -> str:
    repair_context = state.get("repair_context") or {}
    repair_section = ""
    if repair_context:
        repair_section = f"""

REPAIR CONTEXT:
{json.dumps(repair_context, indent=2, default=str)}

This repair run uses a fresh sandbox.
Recreate the required prior computation from the original plan and execution transcript.
Apply the checker fixes by line number, rerun the failed command, and preserve the original user query scope.
"""

    return f"""
USER REQUEST:
{state["user_query"]}

PLANNER STEPS:
{json.dumps(state["plan"], indent=2, default=str)}

VALIDATOR SCRIPT PLAN:
{json.dumps(validation.model_dump(), indent=2, default=str)}

DATASET PROFILE:
{state["dataset_profile"]}

SANDBOX DATASET PATH:
{sandbox_path}

EXECUTION BUDGET:
You may call run_python at most {max_scripts} times.
Each call executes one Python script in the shared sandbox.
Use the exact dataset path and column names from the dataset profile.
Do not return insights or recommendations.
{repair_section}
"""


def exec_agent_node(state: AgentState) -> dict:
    user_query = state.get("user_query", "").strip()
    dataset_profile = state.get("dataset_profile", "").strip()
    plan = state.get("plan", [])

    if not user_query:
        return {
            "execution_status": "error",
            "error": "Exec Agent requires a non-empty user_query.",
        }
    if not dataset_profile:
        return {
            "execution_status": "error",
            "error": "Exec Agent requires the planner dataset_profile.",
        }
    if not plan:
        return {
            "execution_status": "error",
            "error": "Exec Agent requires at least one planner step.",
        }

    try:
        llm = _get_llm()
        validation = _validate_plan(llm, user_query, dataset_profile, plan)
        validation_payload = validation.model_dump()

        if not validation.valid:
            return {
                "validation": validation_payload,
                "execution_status": "error",
                "error": f"Plan validation failed: {validation.reason}",
            }

        max_scripts = max(1, validation.script_count) + 2
    except Exception as exc:
        return {
            "execution_status": "error",
            "error": f"Exec Agent validation failed: {exc}",
        }

    try:
        sandbox, sandbox_path = _prepare_sandbox(state["dataset_path"])
    except Exception as exc:
        return {
            "validation": validation_payload,
            "max_scripts": max_scripts,
            "execution_status": "error",
            "error": f"Exec Agent sandbox preparation failed: {exc}",
        }

    execution_log: dict = {}
    images: list[str] = []
    blocked_attempts: list[dict] = []

    try:
        run_python = _build_run_python(sandbox, execution_log, images)
        agent = create_agent(
            model=llm,
            tools=[run_python],
            system_prompt=EXEC_SYSTEM_PROMPT,
            middleware=[
                CodeSafetyMiddleware(blocked_attempts),
                ToolCallLimitMiddleware(
                    tool_name="run_python",
                    run_limit=max_scripts,
                    exit_behavior="continue",
                ),
            ],
        )
        agent.invoke(
            {
                "messages": [
                    HumanMessage(
                        content=_execution_task(
                            state,
                            validation,
                            sandbox_path,
                            max_scripts,
                        )
                    )
                ]
            }
        )

        execution_status = (
            "limit_reached"
            if len(execution_log) >= max_scripts
            else "completed"
        )
        return {
            "validation": validation_payload,
            "max_scripts": max_scripts,
            "sandbox_path": sandbox_path,
            "execution_log": execution_log,
            "images": images,
            "blocked_attempts": blocked_attempts,
            "execution_status": execution_status,
            "error": "",
        }
    except Exception as exc:
        return {
            "validation": validation_payload,
            "max_scripts": max_scripts,
            "sandbox_path": sandbox_path,
            "execution_log": execution_log,
            "images": images,
            "blocked_attempts": blocked_attempts,
            "execution_status": "error",
            "error": f"Exec Agent failed: {exc}",
        }
    finally:
        sandbox.kill()
