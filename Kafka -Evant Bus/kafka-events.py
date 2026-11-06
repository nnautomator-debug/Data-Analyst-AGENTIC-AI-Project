from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_groq import ChatGroq
from pydantic import BaseModel, Field

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

DATA_DIR = Path(
    os.getenv("KAFKA_DATA_DIR", str(PROJECT_ROOT / "Kafka -Evant Bus/data"))
).expanduser()
LOGGING_DB_PATH = Path(
    os.getenv("LOGGING_DUCKDB_PATH", str(DATA_DIR / "logging.duckdb"))
).expanduser()
EVAL_DB_PATH = Path(
    os.getenv("EVAL_DUCKDB_PATH", str(DATA_DIR / "evals.duckdb"))
).expanduser()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_RUN_TOPIC = os.getenv("KAFKA_RUN_TOPIC", "agent.runs.completed")
KAFKA_LOGGING_GROUP = os.getenv("KAFKA_LOGGING_GROUP", "agent-logging-v1")
KAFKA_EVAL_GROUP = os.getenv("KAFKA_EVAL_GROUP", "agent-evals-v1")
KAFKA_MAX_EVENT_BYTES = int(os.getenv("KAFKA_MAX_EVENT_BYTES", "900000"))
EVAL_JUDGE_MODEL = os.getenv("EVAL_JUDGE_MODEL", "openai/gpt-oss-120b")
MAX_PROCESS_ATTEMPTS = int(os.getenv("KAFKA_MAX_PROCESS_ATTEMPTS", "3"))
JUDGE_MAX_CHARS = int(os.getenv("EVAL_JUDGE_MAX_CHARS", "24000"))
GATE_VERSION = "v1"

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

INFRA_PATTERNS = [
    "kafka",
    "duckdb",
    "groq_api_key",
    "e2b_api_key",
    "api key",
    "authentication",
    "permission denied",
    "rate limit",
    "connectionerror",
    "connection error",
    "could not connect",
    "connection refused",
    "service unavailable",
    "failed to create sandbox",
    "sandbox provisioning",
    "sandbox creation",
]

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOGGER = logging.getLogger("kafka-events")


class JudgeCitation(BaseModel):
    stage: str = ""
    attempt: int = 0
    command: str = ""
    excerpt: str = ""


class JudgeResult(BaseModel):
    verdict: str = "ERROR"
    score: int = 0
    bottlenecks: str = ""
    citations: list[JudgeCitation] = Field(default_factory=list)
    error: str = ""
    raw: dict[str, Any] = Field(default_factory=dict)
    model: str = EVAL_JUDGE_MODEL


class EventEnvelope(BaseModel):
    schema_version: int = 1
    event_id: str
    event_type: str
    produced_at: str
    run: dict


EventEnvelope.model_rebuild()


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json(value: Any) -> str:
    return json.dumps(value, default=str)


def _loads(value: Any, default: Any) -> Any:
    if value is None or value == "":
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return default


def _truncate(value: Any, limit: int) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[:limit] + "\n...[truncated]"


def _ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed
    except Exception:
        return None


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _as_text(value: Any) -> str:
    return str(value or "").strip()


def _as_dict(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list:
    return value if isinstance(value, list) else []


def _normalize_attempt(command: str, entry: Any, attempt: int, source: str) -> dict:
    data = _as_dict(entry)
    images = _as_list(data.get("images"))
    return {
        "attempt": _as_int(attempt),
        "source": source,
        "command": _as_text(command),
        "code": _truncate(data.get("code"), 12000),
        "output": _truncate(data.get("output"), 12000),
        "error": _truncate(data.get("error"), 6000),
        "image_count": len(images),
    }


def _sanitized_execution_log(execution_log: Any) -> dict:
    sanitized = {}
    for command, entry in _as_dict(execution_log).items():
        data = _as_dict(entry)
        images = _as_list(data.get("images"))
        sanitized[str(command)] = {
            "code": _truncate(data.get("code"), 12000),
            "output": _truncate(data.get("output"), 12000),
            "error": _truncate(data.get("error"), 6000),
            "image_count": len(images),
        }
    return sanitized


def _execution_attempts(state: dict) -> list[dict]:
    repair_log = _as_list(state.get("repair_log"))
    attempts: list[dict] = []

    if repair_log:
        first_context = _as_dict(_as_dict(repair_log[0]).get("repair_context"))
        for entry in _as_list(first_context.get("execution_transcript")):
            data = _as_dict(entry)
            attempts.append(
                _normalize_attempt(data.get("command", ""), data, 0, "initial")
            )
        for repair in repair_log:
            repair_data = _as_dict(repair)
            attempt = _as_int(repair_data.get("attempt"))
            for command, entry in _as_dict(repair_data.get("execution_log")).items():
                attempts.append(
                    _normalize_attempt(command, entry, attempt, "repair")
                )

    if not attempts:
        for command, entry in _as_dict(state.get("execution_log")).items():
            attempts.append(_normalize_attempt(command, entry, 0, "initial"))

    return attempts


def _code_check_history(state: dict) -> list[dict]:
    history = []
    for repair in _as_list(state.get("repair_log")):
        repair_data = _as_dict(repair)
        context = _as_dict(repair_data.get("repair_context"))
        history.append(
            {
                "attempt": _as_int(repair_data.get("attempt")),
                "command": _as_text(repair_data.get("command")),
                "checker_cause": _as_text(repair_data.get("checker_cause")),
                "checker_reasoning": _truncate(
                    repair_data.get("checker_reasoning"), 3000
                ),
                "checker_fixes": _as_list(repair_data.get("checker_fixes")),
                "failed_code": _truncate(context.get("failed_code"), 12000),
                "sandbox_error": _truncate(context.get("sandbox_error"), 6000),
                "error_line": _as_int(context.get("error_line")),
                "corrected_code": _truncate(
                    repair_data.get("corrected_code")
                    or context.get("corrected_code"),
                    12000,
                ),
                "execution_status": _as_text(repair_data.get("execution_status")),
                "error": _truncate(repair_data.get("error"), 6000),
                "execution_transcript": _as_list(
                    context.get("execution_transcript")
                ),
            }
        )
    return history


def _repair_count(state: dict) -> int:
    total = 0
    for value in _as_dict(state.get("repair_attempts")).values():
        total += _as_int(value)
    return total


def _code_check_status(state: dict) -> str:
    repair_log = _as_list(state.get("repair_log"))
    if not repair_log:
        return "FAILED" if _as_dict(state.get("code_check")) else "NOT_REQUIRED"
    if state.get("repair_exhausted") or state.get("status") == "failed":
        return "FAILED"
    if any(
        _as_text(_as_dict(repair).get("execution_status")) == "error"
        for repair in repair_log
    ):
        return "ERROR"
    return "REPAIRED"


def _event_bytes(event: dict) -> int:
    return len(_json(event).encode("utf-8"))


def _truncate_event(event: dict, max_bytes: int) -> dict:
    if _event_bytes(event) <= max_bytes:
        return event

    run = event["run"]
    run["event_truncated"] = True
    execution = run["execution"]

    for limit in (6000, 3000, 1500, 800, 400):
        for attempt in execution.get("attempts") or []:
            attempt["output"] = _truncate(attempt.get("output"), limit)
            attempt["error"] = _truncate(attempt.get("error"), limit)
            attempt["code"] = _truncate(attempt.get("code"), max(limit, 2000))
        for item in run.get("code_check_history") or []:
            item["failed_code"] = _truncate(item.get("failed_code"), max(limit, 2000))
            item["corrected_code"] = _truncate(
                item.get("corrected_code"), max(limit, 2000)
            )
            item["sandbox_error"] = _truncate(item.get("sandbox_error"), limit)
            item["checker_reasoning"] = _truncate(
                item.get("checker_reasoning"), limit
            )
        run["final_answer"] = _truncate(run.get("final_answer"), max(limit, 2000))
        run["dataset"]["profile"] = _truncate(
            run["dataset"].get("profile"), max(limit, 2000)
        )
        if _event_bytes(event) <= max_bytes:
            return event

    for limit in (3000, 1000, 400):
        for step in run.get("agent_steps") or []:
            step_state = _as_dict(_as_dict(step).get("state"))
            step_log = _as_dict(step_state.get("execution_log"))
            for entry in step_log.values():
                if isinstance(entry, dict):
                    entry["output"] = _truncate(entry.get("output"), limit)
                    entry["error"] = _truncate(entry.get("error"), limit)
                    entry["code"] = _truncate(entry.get("code"), max(limit, 1000))
        if _event_bytes(event) <= max_bytes:
            return event

    if len(execution.get("attempts") or []) > 3:
        execution["attempts"] = execution["attempts"][-3:]
    if len(run.get("code_check_history") or []) > 2:
        run["code_check_history"] = run["code_check_history"][-2:]
    if _event_bytes(event) <= max_bytes:
        return event

    run["plan"] = []
    run["validation"] = {}
    execution["final_log"] = {}
    execution["blocked_attempts"] = []
    execution["sandbox_failures"] = []
    run["errors"] = [_truncate(error, 500) for error in run.get("errors", [])][-5:]
    run["final_answer"] = _truncate(run.get("final_answer"), 2000)
    run["dataset"]["profile"] = _truncate(run["dataset"].get("profile"), 2000)
    if _event_bytes(event) > max_bytes:
        run["agent_steps"] = []
    return event


def build_run_event(state: dict) -> dict:
    state = _as_dict(state)
    repair_log = _as_list(state.get("repair_log"))
    run_id = _as_text(state.get("job_id")) or uuid.uuid4().hex
    event = {
        "schema_version": 1,
        "event_id": uuid.uuid4().hex,
        "event_type": "agent_run.completed",
        "produced_at": _now(),
        "run": {
            "run_id": run_id,
            "user_query": _as_text(state.get("user_query")),
            "started_at": _as_text(state.get("created_at")),
            "ended_at": _as_text(state.get("updated_at")) or _now(),
            "status": _as_text(state.get("status")),
            "stage": _as_text(state.get("stage")),
            "final_answer": _truncate(state.get("final_answer"), 8000),
            "dataset": {
                "path": _as_text(state.get("dataset_path")),
                "profile": _truncate(state.get("dataset_profile"), 12000),
            },
            "plan": _as_list(state.get("plan")),
            "validation": _as_dict(state.get("validation")),
            "max_scripts": _as_int(state.get("max_scripts")),
            "execution": {
                "status": _as_text(state.get("execution_status")),
                "final_log": _sanitized_execution_log(state.get("execution_log")),
                "attempts": _execution_attempts(state),
                "blocked_attempts": _as_list(state.get("blocked_attempts")),
                "sandbox_failures": _as_list(state.get("sandbox_failures")),
            },
            "code_check_status": _code_check_status(state),
            "code_check_history": _code_check_history(state),
            "repair_attempts": _as_dict(state.get("repair_attempts")),
            "repair_count": _repair_count(state),
            "had_repair": bool(repair_log),
            "repair_exhausted": bool(state.get("repair_exhausted")),
            "errors": _as_list(state.get("errors")),
            "error": _as_text(state.get("error")),
            "timings": _as_dict(state.get("timings")),
            "image_count": len(_as_list(state.get("images"))),
            "agent_steps": _as_list(state.get("agent_steps")),
            "human_feedback": {
                "rating": None,
                "text": None,
                "updated_at": None,
            },
        },
    }
    EventEnvelope.model_validate(event)
    return _truncate_event(event, KAFKA_MAX_EVENT_BYTES)


def _producer_kwargs() -> dict:
    return {
        "bootstrap_servers": [
            item.strip()
            for item in KAFKA_BOOTSTRAP_SERVERS.split(",")
            if item.strip()
        ],
        "acks": "all",
        "compression_type": "gzip",
        "max_request_size": 5 * 1024 * 1024,
        "request_timeout_ms": 30000,
        "max_block_ms": 5000,
    }


def publish_event(event: dict) -> bool:
    if not _env_bool("KAFKA_ENABLED", True):
        LOGGER.info("Kafka publishing is disabled")
        return False

    producer = None
    try:
        EventEnvelope.model_validate(event)
        producer = KafkaProducer(**_producer_kwargs())
        future = producer.send(
            KAFKA_RUN_TOPIC,
            key=str(event["run"]["run_id"]).encode("utf-8"),
            value=_json(event).encode("utf-8"),
            headers=[
                ("event_type", event["event_type"].encode("utf-8")),
                ("schema_version", str(event["schema_version"]).encode("utf-8")),
            ],
        )
        future.get(timeout=30)
        producer.flush(timeout=30)
        LOGGER.info(
            "Published run %s to %s",
            event["run"]["run_id"],
            KAFKA_RUN_TOPIC,
        )
        return True
    except Exception as exc:
        LOGGER.warning("Kafka publish failed: %s", exc)
        return False
    finally:
        if producer is not None:
            try:
                producer.close(timeout=5)
            except Exception:
                LOGGER.debug("Kafka producer close failed", exc_info=True)


def publish_run_completed(state: dict) -> bool:
    return publish_event(build_run_event(state))


def init_topics() -> None:
    admin = None
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=[
                item.strip()
                for item in KAFKA_BOOTSTRAP_SERVERS.split(",")
                if item.strip()
            ],
            client_id="agentic-kafka-admin",
            request_timeout_ms=10000,
        )
        existing = set(admin.list_topics())
        if KAFKA_RUN_TOPIC not in existing:
            admin.create_topics(
                [
                    NewTopic(
                        name=KAFKA_RUN_TOPIC,
                        num_partitions=3,
                        replication_factor=1,
                    )
                ]
            )
            LOGGER.info("Created topic %s", KAFKA_RUN_TOPIC)
        else:
            LOGGER.info("Topic %s already exists", KAFKA_RUN_TOPIC)
    except TopicAlreadyExistsError:
        LOGGER.info("Topic %s already exists", KAFKA_RUN_TOPIC)
    finally:
        if admin is not None:
            try:
                admin.close()
            except Exception:
                LOGGER.debug("Kafka admin close failed", exc_info=True)


def _db_connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    path = Path(db_path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path))


def _ensure_processing_errors(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SEQUENCE IF NOT EXISTS processing_errors_seq START 1")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS processing_errors (
            id BIGINT,
            consumer VARCHAR,
            run_id VARCHAR,
            error VARCHAR,
            raw_event VARCHAR,
            created_at TIMESTAMP
        )
        """
    )


def _ensure_logging_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SEQUENCE IF NOT EXISTS runs_sl_seq START 1")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS runs (
            sl_no BIGINT,
            run_id VARCHAR PRIMARY KEY,
            user_query TEXT,
            agent_started_at TIMESTAMP,
            agent_ended_at TIMESTAMP,
            dataset_path TEXT,
            dataset_profile VARCHAR,
            plan VARCHAR,
            execution_code VARCHAR,
            execution_output VARCHAR,
            execution_error VARCHAR,
            execution_attempts VARCHAR,
            execution_status VARCHAR,
            code_check_status VARCHAR,
            code_check_details VARCHAR,
            synthesis TEXT,
            final_status VARCHAR,
            stage VARCHAR,
            error VARCHAR,
            errors VARCHAR,
            timings VARCHAR,
            had_repair BOOLEAN,
            repair_count INTEGER,
            image_count INTEGER,
            event_produced_at TIMESTAMP,
            logged_at TIMESTAMP,
            schema_version INTEGER,
            raw_event VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS run_steps (
            run_id VARCHAR,
            step_index INTEGER,
            stage VARCHAR,
            status VARCHAR,
            error VARCHAR,
            state_json VARCHAR,
            logged_at TIMESTAMP,
            PRIMARY KEY (run_id, step_index)
        )
        """
    )
    _ensure_processing_errors(conn)


def init_logging_db(db_path: Path = LOGGING_DB_PATH) -> None:
    conn = _db_connect(db_path)
    try:
        _ensure_logging_schema(conn)
    finally:
        conn.close()


def write_run_log(event: dict, db_path: Path = LOGGING_DB_PATH) -> None:
    EventEnvelope.model_validate(event)
    run = event["run"]
    run_id = _as_text(run.get("run_id"))
    execution = _as_dict(run.get("execution"))
    final_log = _as_dict(execution.get("final_log"))
    code_column = {
        command: _as_dict(entry).get("code", "")
        for command, entry in final_log.items()
    }
    output_column = {
        command: _as_dict(entry).get("output", "")
        for command, entry in final_log.items()
    }
    error_column = {
        command: _as_dict(entry).get("error", "")
        for command, entry in final_log.items()
    }
    values = [
        run_id,
        _as_text(run.get("user_query")),
        _ts(run.get("started_at")),
        _ts(run.get("ended_at")),
        _as_text(_as_dict(run.get("dataset")).get("path")),
        _json(_as_dict(run.get("dataset")).get("profile", "")),
        _json(_as_list(run.get("plan"))),
        _json(code_column),
        _json(output_column),
        _json(error_column),
        _json(_as_list(execution.get("attempts"))),
        _as_text(execution.get("status")),
        _as_text(run.get("code_check_status")),
        _json(_as_list(run.get("code_check_history"))),
        _as_text(run.get("final_answer")),
        _as_text(run.get("status")),
        _as_text(run.get("stage")),
        _as_text(run.get("error")),
        _json(_as_list(run.get("errors"))),
        _json(_as_dict(run.get("timings"))),
        bool(run.get("had_repair")),
        _as_int(run.get("repair_count")),
        _as_int(run.get("image_count")),
        _ts(event.get("produced_at")),
        _ts(_now()),
        _as_int(event.get("schema_version"), 1),
        _json(event),
    ]

    conn = _db_connect(db_path)
    try:
        _ensure_logging_schema(conn)
        conn.execute("BEGIN")
        conn.execute(
            """
            INSERT INTO runs (
                sl_no, run_id, user_query, agent_started_at, agent_ended_at,
                dataset_path, dataset_profile, plan, execution_code,
                execution_output, execution_error, execution_attempts,
                execution_status, code_check_status, code_check_details,
                synthesis, final_status, stage, error, errors, timings,
                had_repair, repair_count, image_count, event_produced_at,
                logged_at, schema_version, raw_event
            ) VALUES (
                nextval('runs_sl_seq'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            ON CONFLICT (run_id) DO UPDATE SET
                user_query = excluded.user_query,
                agent_started_at = excluded.agent_started_at,
                agent_ended_at = excluded.agent_ended_at,
                dataset_path = excluded.dataset_path,
                dataset_profile = excluded.dataset_profile,
                plan = excluded.plan,
                execution_code = excluded.execution_code,
                execution_output = excluded.execution_output,
                execution_error = excluded.execution_error,
                execution_attempts = excluded.execution_attempts,
                execution_status = excluded.execution_status,
                code_check_status = excluded.code_check_status,
                code_check_details = excluded.code_check_details,
                synthesis = excluded.synthesis,
                final_status = excluded.final_status,
                stage = excluded.stage,
                error = excluded.error,
                errors = excluded.errors,
                timings = excluded.timings,
                had_repair = excluded.had_repair,
                repair_count = excluded.repair_count,
                image_count = excluded.image_count,
                event_produced_at = excluded.event_produced_at,
                logged_at = excluded.logged_at,
                schema_version = excluded.schema_version,
                raw_event = excluded.raw_event
            """,
            values,
        )
        conn.execute("DELETE FROM run_steps WHERE run_id = ?", [run_id])
        for step_index, step in enumerate(_as_list(run.get("agent_steps"))):
            step_data = _as_dict(step)
            conn.execute(
                """
                INSERT INTO run_steps (
                    run_id, step_index, stage, status, error,
                    state_json, logged_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    run_id,
                    _as_int(step_data.get("step"), step_index),
                    _as_text(step_data.get("stage")),
                    _as_text(step_data.get("status")),
                    _as_text(step_data.get("error")),
                    _json(step_data),
                    _ts(_now()),
                ],
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def _ensure_eval_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SEQUENCE IF NOT EXISTS golden_id_seq START 1")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS eval_runs (
            run_id VARCHAR PRIMARY KEY,
            user_query TEXT,
            agent_started_at TIMESTAMP,
            agent_ended_at TIMESTAMP,
            regex_has_error BOOLEAN,
            regex_final_error BOOLEAN,
            regex_matches VARCHAR,
            human_feedback_rating VARCHAR,
            human_feedback_text TEXT,
            human_feedback_at TIMESTAMP,
            judge_verdict VARCHAR,
            judge_score INTEGER,
            judge_bottlenecks TEXT,
            judge_citations VARCHAR,
            judge_raw VARCHAR,
            judge_model VARCHAR,
            judge_error TEXT,
            judged_at TIMESTAMP,
            bad_case_types VARCHAR,
            primary_bad_case_type VARCHAR,
            failure_origin VARCHAR,
            failure_stage VARCHAR,
            failure_reason TEXT,
            gate_checks VARCHAR,
            missing_requirements VARCHAR,
            golden_status VARCHAR,
            golden_id BIGINT,
            had_repair BOOLEAN,
            repair_count INTEGER,
            raw_event VARCHAR,
            updated_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS golden_dataset (
            golden_id BIGINT,
            run_id VARCHAR UNIQUE,
            added_at TIMESTAMP,
            gate_version VARCHAR,
            bad_case_types VARCHAR,
            primary_bad_case_type VARCHAR,
            failure_origin VARCHAR,
            failure_stage VARCHAR,
            failure_reason TEXT,
            had_repair BOOLEAN,
            repair_count INTEGER,
            human_feedback_rating VARCHAR,
            human_feedback_text TEXT,
            judge_verdict VARCHAR,
            judge_score INTEGER,
            judge_bottlenecks TEXT,
            judge_citations VARCHAR,
            regex_matches VARCHAR,
            user_query TEXT,
            dataset_profile VARCHAR,
            plan VARCHAR,
            execution_attempts VARCHAR,
            code_check_history VARCHAR,
            final_answer TEXT,
            error TEXT,
            timings VARCHAR,
            gate_checks VARCHAR,
            raw_event VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS golden_rejected (
            run_id VARCHAR PRIMARY KEY,
            evaluated_at TIMESTAMP,
            bad_case_types VARCHAR,
            gate_checks VARCHAR,
            missing_requirements VARCHAR,
            rejection_reason TEXT,
            raw_signals VARCHAR
        )
        """
    )
    _ensure_processing_errors(conn)


def init_eval_db(db_path: Path = EVAL_DB_PATH) -> None:
    conn = _db_connect(db_path)
    try:
        _ensure_eval_schema(conn)
    finally:
        conn.close()


def init_dbs() -> None:
    init_logging_db()
    init_eval_db()


def _record_processing_error(
    db_path: Path,
    consumer: str,
    raw_event: Any,
    error: Exception | str,
    initializer: Any,
) -> None:
    run_id = ""
    if isinstance(raw_event, dict):
        run_id = _as_text(_as_dict(raw_event.get("run")).get("run_id"))
    conn = _db_connect(db_path)
    try:
        initializer(conn)
        conn.execute("BEGIN")
        conn.execute(
            """
            INSERT INTO processing_errors (
                id, consumer, run_id, error, raw_event, created_at
            ) VALUES (
                nextval('processing_errors_seq'), ?, ?, ?, ?, ?
            )
            """,
            [
                consumer,
                run_id,
                _truncate(error, 4000),
                _json(raw_event),
                _ts(_now()),
            ],
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def record_logging_error(raw_event: Any, error: Exception | str) -> None:
    _record_processing_error(
        LOGGING_DB_PATH,
        "logging",
        raw_event,
        error,
        _ensure_logging_schema,
    )


def record_eval_error(raw_event: Any, error: Exception | str) -> None:
    _record_processing_error(
        EVAL_DB_PATH,
        "evals",
        raw_event,
        error,
        _ensure_eval_schema,
    )


def detect_regex_errors(event: dict) -> dict:
    run = _as_dict(event.get("run"))
    execution = _as_dict(run.get("execution"))
    matches: list[dict] = []
    seen = set()

    def scan(attempt: Any, command: Any, source: str) -> None:
        data = _as_dict(attempt)
        haystack = "\n".join(
            [
                _as_text(data.get("error")),
                _as_text(data.get("output")),
            ]
        )
        for label, pattern in ERROR_PATTERNS:
            found = pattern.search(haystack)
            if not found:
                continue
            start = max(0, found.start() - 80)
            end = min(len(haystack), found.end() + 120)
            signature = (
                _as_int(data.get("attempt")),
                _as_text(source),
                _as_text(command),
                label,
            )
            if signature in seen:
                continue
            seen.add(signature)
            matches.append(
                {
                    "attempt": _as_int(data.get("attempt")),
                    "source": source,
                    "command": _as_text(command),
                    "pattern": label,
                    "excerpt": _truncate(haystack[start:end], 400),
                }
            )

    for attempt in _as_list(execution.get("attempts")):
        data = _as_dict(attempt)
        scan(data, data.get("command"), _as_text(data.get("source")) or "attempt")

    for command, entry in _as_dict(execution.get("final_log")).items():
        data = _as_dict(entry)
        scan(
            {
                "attempt": "final",
                "error": data.get("error"),
                "output": data.get("output"),
            },
            command,
            "final",
        )

    code_check_history = _as_list(run.get("code_check_history"))
    has_attempt_error = any(
        _as_text(_as_dict(attempt).get("error"))
        for attempt in _as_list(execution.get("attempts"))
    )
    final_log_error = any(
        _as_text(_as_dict(entry).get("error"))
        for entry in _as_dict(execution.get("final_log")).values()
    )
    final_error = bool(
        _as_text(run.get("status")) == "failed"
        or _as_text(execution.get("status")) == "error"
        or _as_text(run.get("error"))
        or final_log_error
    )
    has_error = bool(matches or code_check_history or has_attempt_error or final_log_error)

    return {
        "has_error": has_error,
        "final_error": final_error,
        "matches": matches,
    }


JUDGE_SYSTEM_PROMPT = """
You are a strict evaluator for an agentic data analytics system.
You receive the complete run: user query, plan, all execution attempts including repairs,
code-check history, and final synthesis.
Identify what went wrong or what could materially improve the run.
Focus on agent behavior, not on Kafka, database, API-key, or sandbox-provisioning failures.
Return only valid JSON with exactly these keys:
verdict, score, bottlenecks, citations.
verdict must be PASS or FAIL.
score must be an integer from 1 to 5.
bottlenecks must be at most 100 words.
citations must be a list of objects with stage, attempt, command, and excerpt.
Cite specific evidence from the run.
"""


def _extract_json(text: str) -> dict:
    value = str(text or "").strip()
    if value.startswith("```"):
        value = re.sub(r"^```(?:json)?\s*", "", value)
        value = re.sub(r"\s*```$", "", value)
    start = value.find("{")
    end = value.rfind("}")
    if start < 0 or end < start:
        raise ValueError("Judge response did not contain JSON")
    parsed = json.loads(value[start : end + 1])
    if not isinstance(parsed, dict):
        raise ValueError("Judge response JSON was not an object")
    return parsed


def _normalize_citations(value: Any) -> list[dict]:
    citations = []
    for item in _as_list(value):
        data = _as_dict(item)
        citations.append(
            {
                "stage": _as_text(data.get("stage")),
                "attempt": _as_int(data.get("attempt")),
                "command": _as_text(data.get("command")),
                "excerpt": _truncate(data.get("excerpt"), 500),
            }
        )
    return citations


def judge_run(event: dict, enabled: bool = True) -> dict:
    if not enabled:
        return JudgeResult(
            verdict="SKIPPED",
            score=0,
            bottlenecks="",
            citations=[],
            error="",
            raw={},
        ).model_dump()

    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return JudgeResult(
            verdict="ERROR",
            score=0,
            error="GROQ_API_KEY is missing from the environment.",
            model=EVAL_JUDGE_MODEL,
        ).model_dump()

    llm = ChatGroq(
        model=EVAL_JUDGE_MODEL,
        api_key=api_key,
        temperature=0,
        timeout=120,
        max_retries=2,
    )
    prompt = (
        "RUN EVENT:\n"
        + _truncate(_json(event), JUDGE_MAX_CHARS)
        + "\n\nReturn the required JSON now."
    )
    messages = [
        SystemMessage(content=JUDGE_SYSTEM_PROMPT),
        HumanMessage(content=prompt),
    ]
    last_error = None

    for attempt in range(3):
        try:
            response = llm.invoke(messages)
            content = response.content
            if isinstance(content, list):
                content = "\n".join(
                    str(part.get("text", "")) if isinstance(part, dict) else str(part)
                    for part in content
                )
            parsed = _extract_json(str(content))
            verdict = _as_text(parsed.get("verdict")).upper()
            if verdict not in {"PASS", "FAIL"}:
                raise ValueError(f"Invalid judge verdict: {verdict}")
            score = max(1, min(5, _as_int(parsed.get("score"), 1)))
            bottlenecks = _truncate(parsed.get("bottlenecks"), 4000)
            words = bottlenecks.split()
            if len(words) > 100:
                bottlenecks = " ".join(words[:100])
            result = JudgeResult(
                verdict=verdict,
                score=score,
                bottlenecks=bottlenecks,
                citations=[
                    JudgeCitation(**item)
                    for item in _normalize_citations(parsed.get("citations"))
                ],
                error="",
                raw=parsed,
                model=EVAL_JUDGE_MODEL,
            )
            return result.model_dump()
        except Exception as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(2**attempt)

    return JudgeResult(
        verdict="ERROR",
        score=0,
        error=_truncate(last_error, 2000),
        model=EVAL_JUDGE_MODEL,
    ).model_dump()


def _is_infra_failure(event: dict) -> bool:
    run = _as_dict(event.get("run"))
    execution = _as_dict(run.get("execution"))
    texts = [
        _as_text(run.get("error")),
        _as_text(execution.get("status")),
    ]
    texts.extend(_as_text(error) for error in _as_list(run.get("errors")))
    for attempt in _as_list(execution.get("attempts")):
        texts.append(_as_text(_as_dict(attempt).get("error")))
    for entry in _as_dict(execution.get("final_log")).values():
        texts.append(_as_text(_as_dict(entry).get("error")))
    for item in _as_list(run.get("code_check_history")):
        texts.append(_as_text(_as_dict(item).get("sandbox_error")))
        texts.append(_as_text(_as_dict(item).get("error")))
    haystack = " ".join(texts).lower()
    return any(pattern in haystack for pattern in INFRA_PATTERNS)


def _first_regex_reason(regex: dict) -> str:
    matches = _as_list(regex.get("matches"))
    if not matches:
        return ""
    first = _as_dict(matches[0])
    return _as_text(first.get("excerpt"))


def _derive_failure_stage(
    run: dict,
    regex: dict,
    judge: dict,
    human: dict,
) -> str:
    if bool(run.get("had_repair")):
        return "repair"
    if _as_text(run.get("status")) == "failed":
        return _as_text(run.get("stage")) or "failure"
    if bool(regex.get("final_error")):
        return "exec"
    if _as_text(judge.get("verdict")).upper() == "FAIL":
        return "judge"
    if _as_text(human.get("rating")).lower() == "down" and _as_text(
        human.get("text")
    ):
        return "human_feedback"
    return _as_text(run.get("stage")) or "unknown"


def bad_case_gate(
    event: dict,
    regex: dict,
    judge: dict,
    human_feedback: dict | None = None,
) -> dict:
    run = _as_dict(event.get("run"))
    execution = _as_dict(run.get("execution"))
    human = _as_dict(human_feedback)
    if not human:
        human = _as_dict(run.get("human_feedback"))

    human_rating = _as_text(human.get("rating")).lower()
    human_text = _as_text(human.get("text"))
    human_down = human_rating == "down"
    human_down_with_reason = human_down and bool(human_text)

    judge_verdict = _as_text(judge.get("verdict")).upper()
    judge_error = _as_text(judge.get("error"))
    judge_bottlenecks = _as_text(judge.get("bottlenecks"))
    judge_fail = judge_verdict == "FAIL"
    judge_error_present = judge_verdict == "ERROR" or bool(judge_error)

    status = _as_text(run.get("status"))
    execution_status = _as_text(execution.get("status"))
    run_error = _as_text(run.get("error"))
    regex_final_error = bool(regex.get("final_error"))
    regex_matches = _as_list(regex.get("matches"))
    repair_exhausted = bool(run.get("repair_exhausted"))
    had_repair = bool(run.get("had_repair")) or _as_int(run.get("repair_count")) > 0
    code_check_history = _as_list(run.get("code_check_history"))
    code_check_failed = bool(code_check_history)
    agent_failed = status == "failed" or execution_status == "error" or bool(run_error)

    strong_signals = []
    if human_down_with_reason:
        strong_signals.append("human_down_with_reason")
    if agent_failed:
        strong_signals.append("agent_failed")
    if judge_fail:
        strong_signals.append("judge_fail")
    if regex_final_error:
        strong_signals.append("regex_final_error")
    if repair_exhausted:
        strong_signals.append("repair_exhausted")
    if had_repair:
        strong_signals.append("had_repair")
    if code_check_failed:
        strong_signals.append("code_check_failed")

    priority = [
        "human_down_with_reason",
        "agent_failed",
        "judge_fail",
        "regex_final_error",
        "repair_exhausted",
        "had_repair",
        "code_check_failed",
    ]
    primary = next((item for item in priority if item in strong_signals), "")

    plan = _as_list(run.get("plan"))
    attempts = _as_list(execution.get("attempts"))
    final_answer = _as_text(run.get("final_answer"))
    stage = _derive_failure_stage(run, regex, judge, human)
    plan_ok = bool(plan) or stage in {"profile", "plan"}
    attempts_ok = bool(attempts) or stage in {"profile", "plan"}
    outcome_ok = bool(final_answer or run_error or execution_status)
    stage_ok = stage not in {"", "unknown"}
    reason = (
        human_text
        or _first_regex_reason(regex)
        or judge_bottlenecks
        or run_error
        or _as_text(next(iter(code_check_history), {}).get("checker_reasoning"))
    )
    complete_evidence = bool(
        _as_text(run.get("user_query"))
        and plan_ok
        and attempts_ok
        and outcome_ok
        and stage_ok
        and reason
    )
    actionable = bool(
        regex_matches
        or judge_bottlenecks
        or human_text
        or run_error
        or code_check_history
    )
    infra_failure = _is_infra_failure(event)
    failure_origin = (
        "human_feedback"
        if human_down_with_reason
        and not (agent_failed or judge_fail or regex_final_error or had_repair)
        else "agent_behavior"
    )
    origin_allowed = failure_origin in {"agent_behavior", "human_feedback"}

    checks = {
        "schema_valid": True,
        "has_strong_signal": bool(strong_signals),
        "complete_evidence": complete_evidence,
        "actionable_diagnosis": actionable,
        "failure_origin_allowed": origin_allowed,
        "not_infra_failure": not infra_failure,
    }
    missing = []
    if not checks["has_strong_signal"]:
        missing.append("no_strong_bad_signal")
        if human_down and not human_text:
            missing.append("human_down_missing_reason")
    if not checks["complete_evidence"]:
        missing.append("incomplete_evidence")
    if not checks["actionable_diagnosis"]:
        missing.append("no_actionable_diagnosis")
    if not checks["failure_origin_allowed"]:
        missing.append("failure_origin_not_allowed")
    if not checks["not_infra_failure"]:
        missing.append("infrastructure_failure")
    if judge_error_present and not strong_signals:
        missing.append("judge_error_without_agent_signal")

    qualifies = all(checks.values())
    return {
        "qualifies": qualifies,
        "bad_case_types": strong_signals,
        "primary_bad_case_type": primary,
        "failure_origin": failure_origin,
        "failure_stage": stage,
        "failure_reason": _truncate(reason, 4000),
        "had_repair": had_repair,
        "repair_count": _as_int(run.get("repair_count")),
        "gate_checks": {
            **checks,
            "signals": {
                "human_down": human_down,
                "human_down_with_reason": human_down_with_reason,
                "human_down_missing_reason": human_down and not human_text,
                "agent_failed": agent_failed,
                "regex_final_error": regex_final_error,
                "judge_fail": judge_fail,
                "judge_error": judge_error_present,
                "repair_exhausted": repair_exhausted,
                "had_repair": had_repair,
                "code_check_failed": code_check_failed,
            },
            "infra_failure": infra_failure,
        },
        "missing_requirements": missing,
    }


def _refresh_gate(conn: duckdb.DuckDBPyConnection, run_id: str) -> dict | None:
    row = conn.execute(
        """
        SELECT raw_event, regex_has_error, regex_final_error, regex_matches,
               judge_verdict, judge_score, judge_bottlenecks, judge_citations,
               judge_error, human_feedback_rating, human_feedback_text,
               golden_status, golden_id
        FROM eval_runs
        WHERE run_id = ?
        """,
        [run_id],
    ).fetchone()
    if row is None:
        return None
    if _as_text(row[11]) == "accepted" or row[12] is not None:
        return {"qualifies": True, "already_accepted": True}

    raw_event = _loads(row[0], {})
    if not raw_event:
        conn.execute(
            """
            UPDATE eval_runs
            SET golden_status = 'pending', updated_at = ?
            WHERE run_id = ?
            """,
            [_ts(_now()), run_id],
        )
        return {"qualifies": False, "missing_requirements": ["event_not_available"]}

    regex = {
        "has_error": bool(row[1]),
        "final_error": bool(row[2]),
        "matches": _loads(row[3], []),
    }
    judge = {
        "verdict": _as_text(row[4]),
        "score": _as_int(row[5]),
        "bottlenecks": _as_text(row[6]),
        "citations": _loads(row[7], []),
        "error": _as_text(row[8]),
    }
    human = {
        "rating": _as_text(row[9]),
        "text": _as_text(row[10]),
    }
    gate = bad_case_gate(raw_event, regex, judge, human)
    golden_status = "accepted" if gate["qualifies"] else "rejected"
    conn.execute(
        """
        UPDATE eval_runs
        SET bad_case_types = ?, primary_bad_case_type = ?, failure_origin = ?,
            failure_stage = ?, failure_reason = ?, gate_checks = ?,
            missing_requirements = ?, golden_status = ?, golden_id = NULL,
            updated_at = ?
        WHERE run_id = ?
        """,
        [
            _json(gate["bad_case_types"]),
            gate["primary_bad_case_type"],
            gate["failure_origin"],
            gate["failure_stage"],
            gate["failure_reason"],
            _json(gate["gate_checks"]),
            _json(gate["missing_requirements"]),
            golden_status,
            _ts(_now()),
            run_id,
        ],
    )

    if gate["qualifies"]:
        run = _as_dict(raw_event.get("run"))
        execution = _as_dict(run.get("execution"))
        conn.execute(
            """
            INSERT INTO golden_dataset (
                golden_id, run_id, added_at, gate_version, bad_case_types,
                primary_bad_case_type, failure_origin, failure_stage,
                failure_reason, had_repair, repair_count,
                human_feedback_rating, human_feedback_text, judge_verdict,
                judge_score, judge_bottlenecks, judge_citations, regex_matches,
                user_query, dataset_profile, plan, execution_attempts,
                code_check_history, final_answer, error, timings, gate_checks,
                raw_event
            ) VALUES (
                nextval('golden_id_seq'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            ON CONFLICT (run_id) DO NOTHING
            """,
            [
                run_id,
                _ts(_now()),
                GATE_VERSION,
                _json(gate["bad_case_types"]),
                gate["primary_bad_case_type"],
                gate["failure_origin"],
                gate["failure_stage"],
                gate["failure_reason"],
                bool(gate["had_repair"]),
                _as_int(gate["repair_count"]),
                human.get("rating"),
                human.get("text"),
                judge.get("verdict"),
                _as_int(judge.get("score")),
                judge.get("bottlenecks"),
                _json(judge.get("citations")),
                _json(regex.get("matches")),
                _as_text(run.get("user_query")),
                _json(_as_dict(run.get("dataset")).get("profile", "")),
                _json(_as_list(run.get("plan"))),
                _json(_as_list(execution.get("attempts"))),
                _json(_as_list(run.get("code_check_history"))),
                _as_text(run.get("final_answer")),
                _as_text(run.get("error")),
                _json(_as_dict(run.get("timings"))),
                _json(gate["gate_checks"]),
                _json(raw_event),
            ],
        )
        golden_row = conn.execute(
            "SELECT golden_id FROM golden_dataset WHERE run_id = ?",
            [run_id],
        ).fetchone()
        golden_id = golden_row[0] if golden_row else None
        conn.execute(
            """
            UPDATE eval_runs
            SET golden_status = 'accepted', golden_id = ?
            WHERE run_id = ?
            """,
            [golden_id, run_id],
        )
    else:
        conn.execute(
            """
            INSERT INTO golden_rejected (
                run_id, evaluated_at, bad_case_types, gate_checks,
                missing_requirements, rejection_reason, raw_signals
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (run_id) DO UPDATE SET
                evaluated_at = excluded.evaluated_at,
                bad_case_types = excluded.bad_case_types,
                gate_checks = excluded.gate_checks,
                missing_requirements = excluded.missing_requirements,
                rejection_reason = excluded.rejection_reason,
                raw_signals = excluded.raw_signals
            """,
            [
                run_id,
                _ts(_now()),
                _json(gate["bad_case_types"]),
                _json(gate["gate_checks"]),
                _json(gate["missing_requirements"]),
                ", ".join(gate["missing_requirements"]) or "gate_failed",
                _json({"regex": regex, "judge": judge, "human": human}),
            ],
        )

    return gate


def write_eval_run(
    event: dict,
    regex: dict,
    judge: dict,
    human_feedback: dict | None = None,
    db_path: Path = EVAL_DB_PATH,
) -> dict | None:
    EventEnvelope.model_validate(event)
    run = event["run"]
    human = _as_dict(human_feedback)
    values = [
        _as_text(run.get("run_id")),
        _as_text(run.get("user_query")),
        _ts(run.get("started_at")),
        _ts(run.get("ended_at")),
        bool(regex.get("has_error")),
        bool(regex.get("final_error")),
        _json(_as_list(regex.get("matches"))),
        human.get("rating"),
        human.get("text"),
        _ts(human.get("updated_at")) if human.get("updated_at") else None,
        _as_text(judge.get("verdict")),
        _as_int(judge.get("score")),
        _as_text(judge.get("bottlenecks")),
        _json(_as_list(judge.get("citations"))),
        _json(judge.get("raw") or {}),
        _as_text(judge.get("model")) or EVAL_JUDGE_MODEL,
        _as_text(judge.get("error")),
        _ts(_now()),
        bool(run.get("had_repair")),
        _as_int(run.get("repair_count")),
        _json(event),
        _ts(_now()),
    ]

    conn = _db_connect(db_path)
    try:
        _ensure_eval_schema(conn)
        conn.execute("BEGIN")
        conn.execute(
            """
            INSERT INTO eval_runs (
                run_id, user_query, agent_started_at, agent_ended_at,
                regex_has_error, regex_final_error, regex_matches,
                human_feedback_rating, human_feedback_text, human_feedback_at,
                judge_verdict, judge_score, judge_bottlenecks, judge_citations,
                judge_raw, judge_model, judge_error, judged_at, had_repair,
                repair_count, raw_event, updated_at
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            ON CONFLICT (run_id) DO UPDATE SET
                user_query = excluded.user_query,
                agent_started_at = excluded.agent_started_at,
                agent_ended_at = excluded.agent_ended_at,
                regex_has_error = excluded.regex_has_error,
                regex_final_error = excluded.regex_final_error,
                regex_matches = excluded.regex_matches,
                human_feedback_rating = COALESCE(
                    excluded.human_feedback_rating, eval_runs.human_feedback_rating
                ),
                human_feedback_text = COALESCE(
                    excluded.human_feedback_text, eval_runs.human_feedback_text
                ),
                human_feedback_at = COALESCE(
                    excluded.human_feedback_at, eval_runs.human_feedback_at
                ),
                judge_verdict = excluded.judge_verdict,
                judge_score = excluded.judge_score,
                judge_bottlenecks = excluded.judge_bottlenecks,
                judge_citations = excluded.judge_citations,
                judge_raw = excluded.judge_raw,
                judge_model = excluded.judge_model,
                judge_error = excluded.judge_error,
                judged_at = excluded.judged_at,
                had_repair = excluded.had_repair,
                repair_count = excluded.repair_count,
                raw_event = excluded.raw_event,
                updated_at = excluded.updated_at
            """,
            values,
        )
        gate = _refresh_gate(conn, _as_text(run.get("run_id")))
        conn.execute("COMMIT")
        return gate
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def record_human_feedback(
    run_id: str,
    rating: str,
    text: str = "",
    db_path: Path = EVAL_DB_PATH,
) -> dict | None:
    normalized = _as_text(rating).lower()
    if normalized not in {"up", "down"}:
        raise ValueError("rating must be 'up' or 'down'")

    conn = _db_connect(db_path)
    try:
        _ensure_eval_schema(conn)
        conn.execute("BEGIN")
        existing = conn.execute(
            "SELECT run_id FROM eval_runs WHERE run_id = ?",
            [run_id],
        ).fetchone()
        if existing:
            conn.execute(
                """
                UPDATE eval_runs
                SET human_feedback_rating = ?, human_feedback_text = ?,
                    human_feedback_at = ?, updated_at = ?
                WHERE run_id = ?
                """,
                [
                    normalized,
                    _as_text(text),
                    _ts(_now()),
                    _ts(_now()),
                    run_id,
                ],
            )
        else:
            conn.execute(
                """
                INSERT INTO eval_runs (
                    run_id, human_feedback_rating, human_feedback_text,
                    human_feedback_at, golden_status, updated_at
                ) VALUES (?, ?, ?, ?, 'pending', ?)
                """,
                [
                    run_id,
                    normalized,
                    _as_text(text),
                    _ts(_now()),
                    _ts(_now()),
                ],
            )
        gate = _refresh_gate(conn, run_id)
        conn.execute("COMMIT")
        return gate
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def _consumer_kwargs(group_id: str) -> dict:
    return {
        "bootstrap_servers": [
            item.strip()
            for item in KAFKA_BOOTSTRAP_SERVERS.split(",")
            if item.strip()
        ],
        "group_id": group_id,
        "auto_offset_reset": "earliest",
        "enable_auto_commit": False,
        "max_poll_records": 1,
        "max_poll_interval_ms": 900000,
        "session_timeout_ms": 45000,
        "heartbeat_interval_ms": 3000,
    }


def _event_run_id(event: Any) -> str:
    if isinstance(event, dict):
        return _as_text(_as_dict(event.get("run")).get("run_id"))
    return ""


def _consume_loop(
    consumer: KafkaConsumer,
    handler: Any,
    once: bool,
    label: str,
    error_handler: Any,
) -> int:
    failures: dict[str, int] = {}
    processed = 0
    while True:
        records = consumer.poll(timeout_ms=1000)
        if not records:
            if once:
                break
            continue

        retry_offsets = {}
        for topic_partition, messages in records.items():
            for message in messages:
                event = message.value
                if isinstance(event, (bytes, bytearray)):
                    event = json.loads(event.decode("utf-8"))
                run_id = _event_run_id(event) or (
                    f"{topic_partition.topic}:{topic_partition.partition}:"
                    f"{message.offset}"
                )
                try:
                    handler(event)
                    failures.pop(run_id, None)
                    processed += 1
                    LOGGER.info("%s processed %s", label, run_id)
                except Exception as exc:
                    count = failures.get(run_id, 0) + 1
                    failures[run_id] = count
                    LOGGER.exception(
                        "%s failed for %s attempt %s",
                        label,
                        run_id,
                        count,
                    )
                    if count >= MAX_PROCESS_ATTEMPTS:
                        try:
                            error_handler(event, exc)
                        except Exception:
                            LOGGER.exception(
                                "%s could not record processing error",
                                label,
                            )
                        failures.pop(run_id, None)
                    else:
                        current = retry_offsets.get(topic_partition)
                        if current is None or message.offset < current:
                            retry_offsets[topic_partition] = message.offset

        if retry_offsets:
            for topic_partition, offset in retry_offsets.items():
                consumer.seek(topic_partition, offset)
        else:
            consumer.commit()
    return processed


def run_logging_consumer(once: bool = False) -> int:
    init_logging_db()
    consumer = KafkaConsumer(KAFKA_RUN_TOPIC, **_consumer_kwargs(KAFKA_LOGGING_GROUP))
    try:
        return _consume_loop(
            consumer,
            write_run_log,
            once,
            "logging",
            record_logging_error,
        )
    finally:
        consumer.close()


def run_eval_consumer(once: bool = False, judge_enabled: bool = True) -> int:
    init_eval_db()

    def handler(event: dict) -> None:
        regex = detect_regex_errors(event)
        judge = judge_run(event, enabled=judge_enabled)
        write_eval_run(event, regex, judge)

    consumer = KafkaConsumer(KAFKA_RUN_TOPIC, **_consumer_kwargs(KAFKA_EVAL_GROUP))
    try:
        return _consume_loop(
            consumer,
            handler,
            once,
            "evals",
            record_eval_error,
        )
    finally:
        consumer.close()


def _load_payload(path: str | Path) -> dict:
    payload = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Event file must contain a JSON object")
    if payload.get("run") and payload.get("schema_version"):
        return payload
    return build_run_event(payload)


def _sample_state(job_id: str, **overrides: Any) -> dict:
    state = {
        "job_id": job_id,
        "user_query": "What is the total sales?",
        "dataset_path": "/data/SuperStoreOrders.csv",
        "dataset_profile": "rows: 10\ncolumns: Order ID, Sales",
        "plan": [{"step": 1, "description": "Compute total sales"}],
        "validation": {},
        "max_scripts": 1,
        "sandbox_path": "/sandbox",
        "execution_log": {
            "command_1": {
                "code": "import pandas as pd\nprint(1)",
                "output": "1",
                "error": "",
                "images": ["base64-image-placeholder"],
            }
        },
        "images": ["base64-image-placeholder"],
        "blocked_attempts": [],
        "sandbox_failures": [],
        "execution_status": "completed",
        "active_failure": {},
        "code_check": {},
        "repair_context": {},
        "repair_attempts": {},
        "repair_log": [],
        "max_repair_attempts": 2,
        "repair_exhausted": False,
        "final_answer": "Total sales are 100.",
        "artifacts": [],
        "stage": "synthesis",
        "status": "completed",
        "error": "",
        "errors": [],
        "timings": {"total": 1.2},
        "created_at": "2026-11-04T10:00:00Z",
        "updated_at": "2026-11-04T10:00:05Z",
    }
    state.update(overrides)
    return state


def self_test() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        logging_path = Path(tmp) / "logging.duckdb"
        eval_path = Path(tmp) / "evals.duckdb"
        clean = _sample_state("clean")
        failed = _sample_state(
            "failed",
            status="failed",
            stage="exec",
            execution_status="error",
            error="Exec Agent failed: ValueError: bad column",
            errors=["Exec Agent failed: ValueError: bad column"],
            final_answer="",
            execution_log={
                "command_1": {
                    "code": "print(bad)",
                    "output": "ValueError: bad column",
                    "error": "ValueError: bad column",
                    "images": [],
                }
            },
            images=[],
        )
        repaired = _sample_state(
            "repaired",
            repair_attempts={"command_1": 1},
            repair_log=[
                {
                    "command": "command_1",
                    "attempt": 1,
                    "checker_cause": "wrong column",
                    "checker_reasoning": "The column name was wrong and was corrected.",
                    "checker_fixes": [],
                    "corrected_code": "print(1)",
                    "repair_context": {
                        "failed_code": "print(bad)",
                        "sandbox_error": "ValueError: bad column",
                        "error_line": 1,
                        "execution_transcript": [
                            {
                                "command": "command_1",
                                "code": "print(bad)",
                                "output": "ValueError: bad column",
                                "error": "ValueError: bad column",
                            }
                        ],
                    },
                    "execution_status": "completed",
                    "error": "",
                    "execution_log": {
                        "command_1": {
                            "code": "print(1)",
                            "output": "1",
                            "error": "",
                            "images": [],
                        }
                    },
                }
            ],
        )
        infra = _sample_state(
            "infra",
            status="failed",
            stage="exec",
            execution_status="error",
            error="GROQ_API_KEY is missing from the environment.",
            errors=["GROQ_API_KEY is missing from the environment."],
            final_answer="",
        )
        human_down = _sample_state("human-down")
        human_down_no_text = _sample_state("human-down-no-text")

        cases = [
            (clean, "PASS", None),
            (failed, "FAIL", None),
            (repaired, "PASS", None),
            (infra, "ERROR", None),
            (human_down, "PASS", {"rating": "down", "text": "Wrong scope."}),
            (human_down_no_text, "PASS", {"rating": "down", "text": ""}),
        ]

        events = []
        for state, verdict, human in cases:
            event = build_run_event(state)
            events.append(event)
            serialized = _json(event)
            assert "base64-image-placeholder" not in serialized
            assert event["run"]["run_id"] == state["job_id"]
            write_run_log(event, logging_path)
            regex = detect_regex_errors(event)
            judge = JudgeResult(
                verdict=verdict,
                score=4 if verdict == "PASS" else 1,
                bottlenecks="Judge bottleneck for testing.",
                citations=[],
                error="test judge error" if verdict == "ERROR" else "",
            ).model_dump()
            write_eval_run(
                event,
                regex,
                judge,
                human_feedback=human,
                db_path=eval_path,
            )

        write_run_log(events[0], logging_path)
        write_eval_run(
            events[0],
            detect_regex_errors(events[0]),
            JudgeResult(verdict="PASS", score=4).model_dump(),
            db_path=eval_path,
        )

        logging_conn = _db_connect(logging_path)
        try:
            logging_count = logging_conn.execute(
                "SELECT count(*) FROM runs"
            ).fetchone()[0]
        finally:
            logging_conn.close()

        eval_conn = _db_connect(eval_path)
        try:
            eval_count = eval_conn.execute(
                "SELECT count(*) FROM eval_runs"
            ).fetchone()[0]
            golden_count = eval_conn.execute(
                "SELECT count(*) FROM golden_dataset"
            ).fetchone()[0]
            rejected_count = eval_conn.execute(
                "SELECT count(*) FROM golden_rejected"
            ).fetchone()[0]
            accepted_types = [
                row[0]
                for row in eval_conn.execute(
                    "SELECT bad_case_types FROM golden_dataset ORDER BY run_id"
                ).fetchall()
            ]
        finally:
            eval_conn.close()

        assert logging_count == 6, logging_count
        assert eval_count == 6, eval_count
        assert golden_count == 3, golden_count
        assert rejected_count == 3, rejected_count
        assert any("had_repair" in item for item in accepted_types)

        promoted = record_human_feedback(
            "clean",
            "down",
            "Wrong aggregation.",
            db_path=eval_path,
        )
        assert promoted is not None and promoted["qualifies"]

        eval_conn = _db_connect(eval_path)
        try:
            golden_after = eval_conn.execute(
                "SELECT count(*) FROM golden_dataset"
            ).fetchone()[0]
        finally:
            eval_conn.close()
        assert golden_after == 4, golden_after

        print(
            "self-test passed: "
            f"{logging_count} logs, {eval_count} evals, "
            f"{golden_count} initial golden, {golden_after} after promotion"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-topics")
    subparsers.add_parser("init-db")

    logging_parser = subparsers.add_parser("logging-consumer")
    logging_parser.add_argument("--once", action="store_true")

    eval_parser = subparsers.add_parser("eval-consumer")
    eval_parser.add_argument("--once", action="store_true")
    eval_parser.add_argument("--no-judge", action="store_true")

    produce_parser = subparsers.add_parser("produce")
    produce_parser.add_argument("--event-file", required=True)

    replay_parser = subparsers.add_parser("replay")
    replay_parser.add_argument("--event-file", required=True)
    replay_parser.add_argument("--consumer", choices=["logging", "evals"], required=True)
    replay_parser.add_argument("--no-judge", action="store_true")

    feedback_parser = subparsers.add_parser("feedback")
    feedback_parser.add_argument("--run-id", required=True)
    feedback_parser.add_argument("--rating", choices=["up", "down"], required=True)
    feedback_parser.add_argument("--text", default="")

    subparsers.add_parser("self-test")

    args = parser.parse_args()

    if args.command == "init-topics":
        init_topics()
        return

    if args.command == "init-db":
        init_dbs()
        print(f"initialized {LOGGING_DB_PATH} and {EVAL_DB_PATH}")
        return

    if args.command == "logging-consumer":
        processed = run_logging_consumer(once=args.once)
        print(f"logging consumer processed {processed} events")
        return

    if args.command == "eval-consumer":
        processed = run_eval_consumer(
            once=args.once,
            judge_enabled=not args.no_judge,
        )
        print(f"eval consumer processed {processed} events")
        return

    if args.command == "produce":
        event = _load_payload(args.event_file)
        published = publish_event(event)
        print("published" if published else "not published")
        return

    if args.command == "replay":
        event = _load_payload(args.event_file)
        if args.consumer == "logging":
            write_run_log(event)
            print(f"replayed {event['run']['run_id']} into logging")
        else:
            regex = detect_regex_errors(event)
            judge = judge_run(event, enabled=not args.no_judge)
            gate = write_eval_run(event, regex, judge)
            print(
                f"replayed {event['run']['run_id']} into evals; "
                f"qualifies={bool(gate and gate.get('qualifies'))}"
            )
        return

    if args.command == "feedback":
        gate = record_human_feedback(args.run_id, args.rating, args.text)
        print(_json(gate))
        return

    if args.command == "self-test":
        self_test()
        return


if __name__ == "__main__":
    main()
