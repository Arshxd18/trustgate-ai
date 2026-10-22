from dataclasses import dataclass


@dataclass
class RiskInputs:
    probability_incident_real: float
    average_downtime_minutes: float
    cost_per_minute: float
    sla_penalty: float = 0.0


def calculate_risk(inputs: RiskInputs) -> dict:
    if not 0 <= inputs.probability_incident_real <= 1:
        raise ValueError("Incident probability must be between 0 and 1")

    if inputs.average_downtime_minutes < 0:
        raise ValueError("Downtime cannot be negative")

    if inputs.cost_per_minute < 0 or inputs.sla_penalty < 0:
        raise ValueError("Costs cannot be negative")

    expected_downtime_cost = (
        inputs.probability_incident_real
        * inputs.average_downtime_minutes
        * inputs.cost_per_minute
    )

    expected_sla_cost = (
        inputs.probability_incident_real * inputs.sla_penalty
    )

    expected_loss = expected_downtime_cost + expected_sla_cost

    level = (
        "HIGH" if expected_loss >= 100000
        else "MEDIUM" if expected_loss >= 25000
        else "LOW"
    )

    return {
        "expected_loss_if_ignored": round(expected_loss, 2),
        "expected_downtime_cost": round(expected_downtime_cost, 2),
        "expected_sla_cost": round(expected_sla_cost, 2),
        "risk_level": level,
        "currency": "INR",
        "is_demo_estimate": True,
    }


def calculate_trust_debt(
    false_positives: int = 0,
    operator_overrides: int = 0,
    total_alerts: int = 0,
) -> dict:
    if min(false_positives, operator_overrides, total_alerts) < 0:
        raise ValueError("Counts cannot be negative")

    if false_positives > total_alerts or operator_overrides > total_alerts:
        raise ValueError("Event counts cannot exceed total alerts")

    if total_alerts == 0:
        debt_score = 0.0
    else:
        false_positive_rate = false_positives / total_alerts
        override_rate = operator_overrides / total_alerts

        debt_score = min(
            100.0,
            100 * (
                0.6 * false_positive_rate
                + 0.4 * override_rate
            ),
        )

    level = (
        "HIGH" if debt_score >= 30
        else "MEDIUM" if debt_score >= 10
        else "LOW"
    )

    return {
        "trust_debt_score": round(debt_score, 2),
        "trust_debt_level": level,
        "false_positives": false_positives,
        "operator_overrides": operator_overrides,
        "total_alerts": total_alerts,
        "note": "Demo heuristic; tune with validated operational feedback.",
    }
