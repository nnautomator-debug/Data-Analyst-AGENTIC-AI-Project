import argparse
import json
import os
from pathlib import Path
from typing import Literal, TypedDict

from dotenv import load_dotenv
from e2b_code_interpreter import Sandbox
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

DEFAULT_DATASET_PATH = PROJECT_ROOT / "Data /SuperStoreOrders.csv"
NVIDIA_BASE_URL = "https://integrate.api.nvidia.com/v1"
DEFAULT_NVIDIA_MODEL = "nvidia/nemotron-3.5-bolt-e4b"


class AgentState(TypedDict, total=False):
    dataset_path: str
    user_query: str
    dataset_profile: str
    plan: list[dict]
    error: str


class PlanStep(BaseModel):
    step: str = Field(min_length=3)
    action: str = Field(min_length=10)
    target_agent: Literal["Exec Agent"] = "Exec Agent"


class Plan(BaseModel):
    steps: list[PlanStep] = Field(min_length=1)


Plan.model_rebuild()


PROFILE_CODE = """
import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 240)
df = pd.read_csv(__DATASET_PATH__)

parts = []
parts.append(f"SHAPE: {df.shape}")
parts.append(f"COLUMNS: {df.columns.tolist()}")
parts.append("DTYPES:\\n" + df.dtypes.to_string())
parts.append("MISSING VALUES:\\n" + df.isna().sum().to_string())
parts.append(f"DUPLICATE ROWS: {df.duplicated().sum()}")
parts.append("HEAD:\\n" + df.head(5).to_string())
parts.append("TAIL:\\n" + df.tail(5).to_string())
parts.append("OVERALL DESCRIBE:\\n" + df.describe(include="all").to_string())

for column in df.columns:
    parts.append(f"COLUMN: {column}\\n" + df[column].describe(include="all").to_string())

print("\\n\\n".join(parts))
"""

SYSTEM_PROMPT = """
You are the Planning Agent in a multi-agent data analytics system.
You cannot call tools, run code, or inspect files. The dataset profile below was produced by a sandbox before you ran.
Understand the dataset using its columns, dtypes, head, tail, overall summary, and per-column details.
Convert the user's request into a precise plan for the Exec Agent.
Rules:
- Return exactly the steps required to answer the request.
- Every step must have a short step title, a concrete action, and target_agent set to "Exec Agent".
- Each action must explain the work in 20-30 words.
- Reference only columns that exist in the dataset profile.
- Include data retrieval, computation, visualization when useful, and final insight generation.
- Do not invent data, values, or conclusions.
- Return only valid JSON. Do not include markdown fences or commentary.

CRITICAL RULES FOR TIME-BASED QUERIES:
- For "which month", "which year", "monthly trend", "yearly trend" questions: ALWAYS group by year-month (calendar month) using `df['order_date'].dt.to_period('M')` or `df['order_date'].dt.strftime('%Y-%m')`, NOT by month number alone (`dt.month`). Month number loses year information and aggregates across years incorrectly.
- For date parsing: ALWAYS use `pd.to_datetime(df['order_date'], format='mixed', dayfirst=True, errors='coerce')` — the dataset uses day-first format (DD/MM/YYYY).
- For CSV reading: ALWAYS use `pd.read_csv(path, thousands=',')` — numeric columns may contain comma-formatted numbers.
- For order counts: ALWAYS use `df['order_id'].nunique()` — `order_id` is not unique per row (multiple line items per order).
- For profit margin: Compute as `sum(profit) / sum(sales)` per group, not average of row-level margins.
- For "which is doing best" and "top 5" questions: First filter the requested scope with equality (e.g., `df['category'] == 'Technology'`) and then rank the requested entity; never group by the broader scope column instead of filtering it.
- For ranking and aggregation tasks: Plan the exact grouping key, metric formula, and filters explicitly; group only by the entity being ranked (e.g., `product_name`) and do not add extra grouping keys or filters that change the requested population.

NEGATIVE EXAMPLES (DO NOT REPEAT THESE MISTAKES):

❌ BAD PLAN for "Which month had the highest total sales?":
  Step: "Extract Month" → Action: "Convert order_date to datetime and extract month number (dt.month) for grouping"
  Step: "Calculate Monthly Sales" → Action: "Group by month number and sum sales"
  Step: "Find Highest Sales Month" → Action: "Find month with max sales"
  WHY WRONG: Groups by month number (1-12) across ALL years. Returns November (month 11) with sales=988,420 (sum of all Novembers). Correct answer is 2014-11 with sales=555,312.

❌ BAD PLAN for "Which month had the lowest total profit?":
  Step: "Extract Month" → Action: "Convert order_date to datetime and extract month number (dt.month) for grouping"
  Step: "Calculate Monthly Profit" → Action: "Group by month number and sum profit"
  Step: "Find Lowest Profit Month" → Action: "Find month with min profit"
  WHY WRONG: Groups by month number (1-12) across ALL years. Returns February (month 2) with profit=72,510. Correct answer is 2011-07 with profit=5,585.

✅ CORRECT APPROACH for time-based "which month" questions:
  Step: "Parse Dates" → Action: "Parse order_date with pd.to_datetime(format='mixed', dayfirst=True, errors='coerce')"
  Step: "Create Year-Month" → Action: "Create year-month period column: df['year_month'] = df['order_date'].dt.to_period('M')"
  Step: "Aggregate by Year-Month" → Action: "Group by year_month and sum sales/profit"
  Step: "Find Extreme" → Action: "Identify the year-month with max/min value"

{format_instructions}
"""

USER_PROMPT = """
User request:
{user_query}

Dataset profile:
{dataset_profile}
"""


def _profile_code(sandbox_path: str) -> str:
    return PROFILE_CODE.replace("__DATASET_PATH__", json.dumps(sandbox_path))


def profile_dataset(dataset_path: str | Path) -> str:
    path = Path(dataset_path).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Dataset not found: {path}")

    sandbox_path = f"/home/user/{path.name}"
    sandbox = Sandbox.create()

    try:
        sandbox.files.write(
            sandbox_path,
            path.read_bytes(),
            request_timeout=120,
        )
        execution = sandbox.run_code(_profile_code(sandbox_path), timeout=120)
        stdout = "".join(execution.logs.stdout).strip()
        stderr = "".join(execution.logs.stderr).strip()

        if execution.error:
            raise RuntimeError(
                f"Sandbox execution failed: "
                f"{getattr(execution.error, 'name', 'Error')}: "
                f"{getattr(execution.error, 'value', execution.error)}"
            )
        if not stdout:
            raise RuntimeError("Sandbox returned an empty dataset profile.")

        return f"{stdout}\n\nSTDERR:\n{stderr}" if stderr else stdout
    finally:
        sandbox.kill()


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


def _generate_plan(user_query: str, dataset_profile: str) -> Plan:
    parser = JsonOutputParser(pydantic_object=Plan)
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", SYSTEM_PROMPT),
            ("human", USER_PROMPT),
        ]
    ).partial(format_instructions=parser.get_format_instructions())

    chain = prompt | _get_llm() | parser
    return Plan.model_validate(
        chain.invoke(
            {
                "user_query": user_query,
                "dataset_profile": dataset_profile,
            }
        )
    )


def planning_node(state: AgentState) -> dict:
    user_query = state.get("user_query", "").strip()
    if not user_query:
        return {"error": "Planning agent requires a non-empty user_query."}

    try:
        dataset_profile = state.get("dataset_profile") or profile_dataset(
            state["dataset_path"]
        )
        plan = _generate_plan(user_query, dataset_profile)
        return {
            "dataset_profile": dataset_profile,
            "plan": [step.model_dump() for step in plan.steps],
            "error": "",
        }
    except Exception as exc:
        return {"error": f"Planning agent failed: {exc}"}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", nargs="?", default=str(DEFAULT_DATASET_PATH))
    parser.add_argument(
        "query",
        nargs="?",
        default="Analyze this dataset and produce the key business insights.",
    )
    args = parser.parse_args()

    result = planning_node(
        {
            "dataset_path": args.dataset,
            "user_query": args.query,
        }
    )
    print(json.dumps(result.get("plan", result), indent=2))


if __name__ == "__main__":
    main()
