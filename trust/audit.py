import json
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

AUDIT_PATH = Path.home() / "trustgate" / "logs" / "audit.jsonl"
LOCK = threading.Lock()


def record_decision(
    incident_id: str,
    decision: str,
    operator: str,
    reason: str = "",
    trust_score: float | None = None,
    risk_level: str | None = None,
) -> dict:
    decision = decision.strip().lower()

    if decision not in {"approve", "reject"}:
        raise ValueError("Decision must be approve or reject")

    if not incident_id.strip():
        raise ValueError("Incident ID is required")

    if not operator.strip():
        raise ValueError("Operator is required")

    if decision == "reject" and not reason.strip():
        raise ValueError("A rejection reason is required")

    event = {
        "audit_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "incident_id": incident_id,
        "decision": decision,
        "operator": operator,
        "reason": reason.strip(),
        "trust_score": trust_score,
        "risk_level": risk_level,
        "action_executed": False,
    }

    AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with LOCK:
        with AUDIT_PATH.open("a", encoding="utf-8") as file:
            file.write(json.dumps(event) + "\n")

    return event


def get_audit_history(limit: int = 50) -> list[dict]:
    if not 1 <= limit <= 500:
        raise ValueError("Limit must be between 1 and 500")

    if not AUDIT_PATH.exists():
        return []

    with LOCK:
        with AUDIT_PATH.open(encoding="utf-8") as file:
            lines = file.readlines()

    events = []
    for line in lines[-limit:]:
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    return list(reversed(events))
