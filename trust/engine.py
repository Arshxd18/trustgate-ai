from dataclasses import dataclass, asdict


@dataclass
class TrustSignals:
    model_confidence: float
    historical_accuracy: float
    category_accuracy: float
    operator_acceptance: float
    data_quality: float
    model_freshness: float
    sample_size: int = 0


def calculate_trust(signals: TrustSignals) -> dict:
    """
    Demo trust score.
    All percentage-like inputs must be between 0 and 100.
    """
    values = asdict(signals)
    percentage_fields = [
        "model_confidence",
        "historical_accuracy",
        "category_accuracy",
        "operator_acceptance",
        "data_quality",
        "model_freshness",
    ]

    for field in percentage_fields:
        value = values[field]
        if not 0 <= value <= 100:
            raise ValueError(f"{field} must be between 0 and 100")

    # Sample-size-aware operator acceptance.
    # With little feedback, the signal is pulled toward a neutral 50%.
    prior_strength = 20
    effective_acceptance = (
        signals.operator_acceptance * signals.sample_size
        + 50 * prior_strength
    ) / (signals.sample_size + prior_strength)

    weights = {
        "model_confidence": 0.15,
        "historical_accuracy": 0.25,
        "category_accuracy": 0.20,
        "operator_acceptance": 0.15,
        "data_quality": 0.15,
        "model_freshness": 0.10,
    }

    score = (
        signals.model_confidence * weights["model_confidence"]
        + signals.historical_accuracy * weights["historical_accuracy"]
        + signals.category_accuracy * weights["category_accuracy"]
        + effective_acceptance * weights["operator_acceptance"]
        + signals.data_quality * weights["data_quality"]
        + signals.model_freshness * weights["model_freshness"]
    )

    level = "HIGH" if score >= 80 else "MEDIUM" if score >= 60 else "LOW"

    return {
        "trust_score": round(score, 2),
        "trust_level": level,
        "effective_operator_acceptance": round(effective_acceptance, 2),
        "weights": weights,
        "signals": values,
        "note": "Demo score; calibrate weights using validated operational feedback.",
    }
