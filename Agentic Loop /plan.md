# Planning Agent

## Role

The planning agent is the first node in the data-analytics workflow. It receives the user request and a profile of the uploaded dataset, then produces a step-by-step plan for the Exec Agent.

The planning agent has no tools and cannot execute code. Dataset inspection happens before the LLM is called, inside the same file, through the E2B sandbox.

## Runtime Flow

1. Load environment variables from the project `.env` file.
2. Upload the user's dataset to an E2B sandbox.
3. Run pandas profiling inside the sandbox.
4. Collect shape, columns, dtypes, missing values, duplicate count, head, tail, overall describe, and per-column describe.
5. Pass the profile and user request to the NVIDIA-hosted LLM through the OpenAI-compatible endpoint.
6. Return a structured plan for the Exec Agent.

## State

```python
class AgentState(TypedDict, total=False):
    dataset_path: str
    user_query: str
    dataset_profile: str
    plan: list[dict]
    error: str
```

## Output Contract

```json
{
  "steps": [
    {
      "step": "Filter November and December profit",
      "action": "Filter sales and profit for November and December, grouped by month and product category.",
      "target_agent": "Exec Agent"
    },
    {
      "step": "Chart monthly profit",
      "action": "Generate a matplotlib line chart comparing November and December profit trends over time.",
      "target_agent": "Exec Agent"
    },
    {
      "step": "Report profit insights",
      "action": "Analyze the computed values and chart, then report the main trends and business insights.",
      "target_agent": "Exec Agent"
    }
  ]
}
```

The planner must not return `expected_output`, commentary, markdown fences, or extra keys.

## Planning Rules

- Steps must be specific to the uploaded dataset and the user request.
- Only columns present in the E2B profile may be referenced.
- Actions should be 20-30 words.
- Include data retrieval, computation, visualization when useful, and final insight generation.
- Do not invent values, columns, or conclusions.
- All steps target the Exec Agent.

## Environment

Required variables in `.env`:

```dotenv
E2B_API_KEY=...
NVIDIA_API_KEY=...
```

Optional:

```dotenv
NVIDIA_MODEL=nvidia/nemotron-3.5-bolt-e4b
NVIDIA_BASE_URL=https://integrate.api.nvidia.com/v1
```

## Run

```bash
python "Agentic Loop /plan-agent.py"
```

With a custom dataset and request:

```bash
python "Agentic Loop /plan-agent.py" "Data /SuperStoreOrders.csv" "Compare November and December profit."
```

## Current Boundary

This file contains only the planning node and its required preprocessing. The main LangGraph workflow and the Exec Agent will be added later.

# Plan + Exec Agent Pair

## Overview

The **Planning Agent** and **Exec Agent** form a two-node pipeline in the LangGraph workflow:

| Node | Responsibility | Input | Output |
|------|----------------|-------|--------|
| **Planning Agent** | Analyze dataset profile + user request → produce ordered steps | `dataset_path`, `user_query` | `plan: list[dict]`, `dataset_profile: str` |
| **Exec Agent** | Validate plan → execute Python scripts in E2B sandbox → return data & charts | `plan`, `dataset_profile`, `user_query`, `dataset_path` | `execution_log`, `images[]`, `validation`, `execution_status` |

## Data Flow

```
User Request + Dataset
        │
        ▼
┌───────────────────┐
│  Planning Agent   │  (no tools, LLM-only)
│  - Profile dataset│
│  - Generate steps │
└─────────┬─────────┘
          │ plan[] + dataset_profile
          ▼
┌───────────────────┐
│    Exec Agent     │  (tools: run_python)
│  - Validate plan  │
│  - Execute code   │
│  - Capture output │
└─────────┬─────────┘
          │ execution_log + images
          ▼
   (Future: Insight Agent)
```

## Shared State Keys

Both nodes read/write the same `AgentState` (defined in each file). Key shared fields:

| Field | Planning Agent | Exec Agent |
|-------|----------------|------------|
| `dataset_path` | writes | reads |
| `user_query` | reads | reads |
| `dataset_profile` | writes | reads |
| `plan` | writes | reads |
| `validation` | — | writes |
| `execution_log` | — | writes |
| `images` | — | writes |
| `execution_status` | — | writes |

## Contract Between Nodes

The Planning Agent **must** produce steps that:
- Reference only columns present in `dataset_profile`
- Are specific enough for the Exec Agent to write executable pandas/matplotlib code
- Target `"Exec Agent"` in each step's `target_agent` field

The Exec Agent **must**:
- Validate the plan before execution (via Validator chain)
- Never invent columns, values, or insights
- Return only computational artifacts (DataFrames, stats, base64 PNGs)

## Run Together (Future)

When the main LangGraph workflow is added, the pair will be wired as:

```python
workflow = StateGraph(AgentState)
workflow.add_node("planner", planning_agent_node)
workflow.add_node("executor", exec_agent_node)
workflow.add_edge("planner", "executor")
workflow.set_entry_point("planner")
```

---

# Exec Agent

## Role

The Exec Agent is the second node in the data-analytics workflow. It receives the planner's step-by-step plan, validates it, and then executes Python code in an E2B sandbox to produce data, computations, and visualizations. It does **not** generate business insights or final answers—only executable artifacts (DataFrames, statistics, charts).

---

## Runtime Flow

1. **Plan Validation**  
   A validator LLM chain (LangChain + NVIDIA OpenAI-compatible endpoint) reviews the planner steps against the user request and dataset profile. It outputs a structured `ValidationResult` containing:
   - `valid: bool` — whether the plan is feasible with the given dataset
   - `script_count: int` — estimated number of Python scripts needed
   - `scripts: list[ValidationScript]` — each with `script` (1-based index), `purpose` (what the script does), and `steps_covered` (which planner steps it addresses)

2. **Execution Budget**  
   `max_scripts = max(1, validation.script_count) + 2` (the `+2` is a safety buffer). This limit is enforced by LangChain's `ToolCallLimitMiddleware` on the single `run_python` tool.

3. **Sandbox Preparation**  
   - The user's dataset is uploaded once to a fresh E2B sandbox (`/home/user/<filename>`).
   - The sandbox path is passed to the Exec Agent so it never re-uploads data.

4. **Execution Loop**  
   The Exec Agent (a LangChain `create_agent` with the `run_python` tool) runs in a loop:
   - Receives the planner steps, validator script plan, dataset profile, sandbox path, and budget.
   - Generates a self-contained Python script for the next step(s).
   - Calls `run_python(code)` → code executes in the shared sandbox.
   - The tool **automatically** appends a JSON entry to `execution_log`:
     ```json
     {
       "command_1": {
         "code": "...",
         "output": "STDOUT/RESULT TEXT/STDERR/ERROR",
         "error": null | "Traceback...",
         "images": ["base64_png_1", "..."]
       }
     }
     ```
   - PNG images (matplotlib/seaborn figures via `plt.show()`) are captured as base64 strings and accumulated in `images[]`.
   - Loop continues until the agent stops calling tools or the budget is exhausted.

5. **Safety Middleware** (`CodeSafetyMiddleware`)  
   Before any code reaches the sandbox, a regex-based middleware blocks:
   - IPython shell escapes (`!`, `%%bash`, `%system`, etc.)
   - `os.system`, `os.popen`, `subprocess.*`, `pty.spawn`
   - `shell=True`, explicit shell binaries (`bash`, `zsh`, `powershell`, `cmd.exe`)
   - Network commands (`curl`, `wget`), destructive commands (`rm -rf`), `sudo`
   Blocked attempts are recorded in `blocked_attempts[]` with the matched pattern, label, and offending code snippet. The agent receives a tool error message and must rewrite.

6. **Output Contract**  
   On completion, the node returns a state update with:
   ```python
   {
     "validation": ValidationResult.model_dump(),
     "max_scripts": int,
     "sandbox_path": str,
     "execution_log": dict,      # command_N → {code, output, error, images}
     "images": list[str],        # all base64 PNGs in order
     "blocked_attempts": list[dict],
     "execution_status": "completed" | "limit_reached" | "error",
     "error": str
   }
   ```

---

## State

```python
class AgentState(TypedDict, total=False):
    dataset_path: str
    user_query: str
    dataset_profile: str
    plan: list[dict]
    validation: dict              # ValidationResult
    max_scripts: int
    sandbox_path: str
    execution_log: dict           # command_N → {code, output, error, images}
    images: list[str]             # base64 PNG strings
    blocked_attempts: list[dict]  # {pattern, match, code}
    execution_status: str
    error: str
```

---

## Validator Output Schema

```json
{
  "valid": true,
  "script_count": 3,
  "scripts": [
    {
      "script": 1,
      "purpose": "Load dataset, parse dates, filter November and December rows, compute monthly profit by category",
      "steps_covered": ["Filter November and December profit"]
    },
    {
      "script": 2,
      "purpose": "Generate matplotlib line chart comparing November vs December profit trends over time",
      "steps_covered": ["Chart monthly profit"]
    },
    {
      "script": 3,
      "purpose": "Compute summary statistics and prepare data for final insight generation",
      "steps_covered": ["Report profit insights"]
    }
  ],
  "reason": ""
}
```

---

## Exec Agent System Prompt (Key Rules)

- **Only** use pandas, numpy, matplotlib, seaborn — no bash/shell/subprocess/os.system.
- Use the exact column names and categorical values from the dataset profile.
- Use the prepared `sandbox_path` for `pd.read_csv`; never re-upload.
- Handle comma-formatted numbers: `pd.read_csv(..., thousands=",")` or strip commas before `pd.to_numeric`.
- Never silently accept `errors="coerce"` data loss — fix parsing instead.
- Parse dates with `pd.to_datetime(format="mixed", dayfirst=True, errors="coerce")` unless profile proves a single format.
- Use `.copy()` after filtering; create derived date columns before `groupby`.
- Call `plt.show()` so figures are captured as PNG.
- Stop when required data/images are produced or budget exhausted.
- **Never** return insights, recommendations, or final answers.

---

## Safety Middleware Patterns

| Pattern | Label |
|---------|-------|
| `^\s*!(?!=)` | IPython shell escape |
| `^\s*%%(?:bash\|sh\|shell\|script)\b` | IPython shell magic |
| `^\s*%(?:system\|sx\|bash\|sh\|shell)\b` | IPython system magic |
| `os\.(?:system\|popen\|spawn\w*\|exec\w*)\s*\(` | OS process execution |
| `subprocess\.\w+\s*\(` | subprocess execution |
| `pty\.spawn\s*\(` | pty execution |
| `shell\s*=\s*True\b` | shell=True |
| `\b(?:bash\|zsh\|fish\|powershell\|cmd\.exe)\b` | shell binary |
| `\b(?:curl\|wget)\b` | network shell command |
| `rm\s+-rf\b` | destructive shell command |
| `sudo\b` | privileged shell command |

---

## Environment

Same as Planning Agent (`.env`):

```dotenv
E2B_API_KEY=...
NVIDIA_API_KEY=...
NVIDIA_MODEL=nvidia/nemotron-3.5-super-vl-120b-a12b   # or override
NVIDIA_BASE_URL=https://integrate.api.nvidia.com/v1
```

---

## Run

The Exec Agent is invoked as a LangGraph node (`exec_agent_node`). A minimal standalone test:

```bash
python "Agentic Loop /exec-agent.py" "Data /SuperStoreOrders.csv" "Compare November and December profit."
```

(Assumes `plan` and `dataset_profile` are pre-populated in state; normally called from the main workflow.)

---

## Current Boundary

This file contains the complete Exec Agent node:
- Validator chain
- Sandbox management
- `run_python` tool with automatic execution logging
- `CodeSafetyMiddleware`
- `ToolCallLimitMiddleware` budget enforcement
- LangGraph-compatible `exec_agent_node(state) -> dict`

The main LangGraph workflow (wiring Planning Agent → Exec Agent → future Insight Agent) will be added later.