from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

DETECTION_THRESHOLD = 0.25
from pydantic import BaseModel, Field
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

from trust.engine import TrustSignals, calculate_trust
from trust.risk_engine import RiskInputs, calculate_risk, calculate_trust_debt
from trust.audit import record_decision, get_audit_history

MODEL_PATH = str(
    Path(__file__).resolve().parent.parent
    / "models" / "iot23_random_forest_v1" / "pipeline_model"
)

app = FastAPI(
    title="TrustGate API",
    description="AI prediction and operational trust governance",
    version="0.3.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://trustgatefrontend.vercel.app",
        "http://localhost:5173",
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

spark = (
    SparkSession.builder
    .appName("TrustGateInference")
    .master("local[1]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.driver.memory", "768m")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
model = PipelineModel.load(MODEL_PATH)

INPUT_SCHEMA = StructType([
    StructField("src_port", IntegerType(), True),
    StructField("dst_port", IntegerType(), True),
    StructField("proto", StringType(), True),
    StructField("service", StringType(), True),
    StructField("duration", DoubleType(), True),
    StructField("orig_bytes", DoubleType(), True),
    StructField("resp_bytes", DoubleType(), True),
    StructField("conn_state", StringType(), True),
    StructField("local_orig", StringType(), True),
    StructField("local_resp", StringType(), True),
    StructField("missed_bytes", DoubleType(), True),
    StructField("history", StringType(), True),
    StructField("orig_pkts", DoubleType(), True),
    StructField("orig_ip_bytes", DoubleType(), True),
    StructField("resp_pkts", DoubleType(), True),
    StructField("resp_ip_bytes", DoubleType(), True),
    StructField("label", StringType(), True),
    StructField("traffic_measurements_missing", IntegerType(), True),
    StructField("duration_missing", IntegerType(), True),
    StructField("orig_bytes_missing", IntegerType(), True),
    StructField("resp_bytes_missing", IntegerType(), True),
    StructField("event_hour", IntegerType(), True),
    StructField("event_day_of_week", IntegerType(), True),
])


class TrafficInput(BaseModel):
    src_port: int = 443
    dst_port: int = 55555
    proto: str = "tcp"
    service: str = "ssl"
    duration: Optional[float] = 1.2
    orig_bytes: Optional[float] = 500.0
    resp_bytes: Optional[float] = 1200.0
    conn_state: str = "SF"
    local_orig: str = "unknown"
    local_resp: str = "unknown"
    missed_bytes: Optional[float] = 0.0
    history: str = "unknown"
    orig_pkts: Optional[float] = 5.0
    orig_ip_bytes: Optional[float] = 780.0
    resp_pkts: Optional[float] = 6.0
    resp_ip_bytes: Optional[float] = 1450.0
    event_hour: int = 12
    event_day_of_week: int = 2


class AnalyzeRequest(TrafficInput):
    incident_id: str = "DEMO-001"
    historical_accuracy: float = Field(75, ge=0, le=100)
    category_accuracy: float = Field(70, ge=0, le=100)
    operator_acceptance: float = Field(80, ge=0, le=100)
    data_quality: float = Field(90, ge=0, le=100)
    model_freshness: float = Field(85, ge=0, le=100)
    feedback_sample_size: int = Field(5, ge=0)
    average_downtime_minutes: float = Field(60, ge=0)
    cost_per_minute: float = Field(1000, ge=0)
    sla_penalty: float = Field(10000, ge=0)
    false_positives: int = Field(0, ge=0)
    operator_overrides: int = Field(0, ge=0)
    total_alerts: int = Field(0, ge=0)


class ReviewRequest(BaseModel):
    incident_id: str
    decision: str
    operator: str
    reason: str = ""
    trust_score: Optional[float] = Field(None, ge=0, le=100)
    risk_level: Optional[str] = None


def run_prediction(traffic: TrafficInput) -> dict:
    values = traffic.model_dump()

    measured_fields = ["duration", "orig_bytes", "resp_bytes"]
    values["traffic_measurements_missing"] = int(
        any(values[field] is None for field in measured_fields)
    )
    values["duration_missing"] = int(values["duration"] is None)
    values["orig_bytes_missing"] = int(values["orig_bytes"] is None)
    values["resp_bytes_missing"] = int(values["resp_bytes"] is None)

    # Required input to the saved pipeline; this is not the output prediction.
    values["label"] = "benign"

    result = model.transform(
        spark.createDataFrame([values], schema=INPUT_SCHEMA)
    ).select("prediction", "probability").first()

    probabilities = result["probability"].toArray()
    prediction = "malicious" if int(result["prediction"]) == 1 else "benign"

    return {
        "model_prediction": ("malicious" if int(result["prediction"]) == 1 else "benign"),
        "prediction": ("malicious" if float(probabilities[1]) >= DETECTION_THRESHOLD else "benign"),
        "detection_threshold": DETECTION_THRESHOLD,
        "malicious_probability": float(probabilities[1]),
        "benign_probability": float(probabilities[0]),
        "model": "iot23_random_forest_v1",
    }


@app.get("/")
def home():
    return {
        "project": "TrustGate",
        "status": "online",
        "message": "AI operational trust layer is running",
    }


@app.get("/health")
def health():
    return {"status": "healthy", "model_loaded": model is not None}


@app.get("/model-info")
def model_info():
    return {
        "model": "Random Forest",
        "trees": 80,
        "max_depth": 12,
        "features": 170,
        "model_loaded": model is not None,
    }


@app.post("/predict")
def predict(traffic: TrafficInput):
    try:
        result = run_prediction(traffic)
        result["warning"] = (
            "A benign prediction does not prove traffic is safe."
        )
        return result
    except Exception:
        raise HTTPException(500, "Prediction failed; inspect the API log.")


@app.post("/analyze")
def analyze(request: AnalyzeRequest):
    try:
        prediction = run_prediction(request)
        malicious_probability = prediction["malicious_probability"]

        trust = calculate_trust(TrustSignals(
            model_confidence=max(
                prediction["malicious_probability"],
                prediction["benign_probability"],
            ) * 100,
            historical_accuracy=request.historical_accuracy,
            category_accuracy=request.category_accuracy,
            operator_acceptance=request.operator_acceptance,
            data_quality=request.data_quality,
            model_freshness=request.model_freshness,
            sample_size=request.feedback_sample_size,
        ))

        risk = calculate_risk(RiskInputs(
            probability_incident_real=malicious_probability,
            average_downtime_minutes=request.average_downtime_minutes,
            cost_per_minute=request.cost_per_minute,
            sla_penalty=request.sla_penalty,
        ))

        debt = calculate_trust_debt(
            false_positives=request.false_positives,
            operator_overrides=request.operator_overrides,
            total_alerts=request.total_alerts,
        )

        if (
            malicious_probability >= DETECTION_THRESHOLD
            or trust["trust_score"] < 60
            or risk["risk_level"] == "HIGH"
        ):
            recommendation = "HUMAN_REVIEW"
        else:
            recommendation = "REVIEW_REQUIRED_BEFORE_ACTION"

        return {
            "incident_id": request.incident_id,
            "prediction": prediction["prediction"],
            "model_prediction": prediction["model_prediction"],
            "detection_threshold": DETECTION_THRESHOLD,
            "prediction_details": prediction,
            "trust": trust,
            "risk": risk,
            "trust_debt": debt,
            "recommendation": recommendation,
            "human_approval_required": True,
            "automatic_action_executed": False,
            "demo_assumptions": {
                "trust_history_is_demo_input": True,
                "risk_costs_are_configurable_estimates": True,
            },
        }
    except ValueError as error:
        raise HTTPException(422, str(error))
    except Exception:
        raise HTTPException(500, "Analysis failed; inspect the API log.")


@app.post("/review")
def review(request: ReviewRequest):
    try:
        return record_decision(
            incident_id=request.incident_id,
            decision=request.decision,
            operator=request.operator,
            reason=request.reason,
            trust_score=request.trust_score,
            risk_level=request.risk_level,
        )
    except ValueError as error:
        raise HTTPException(422, str(error))
    except Exception:
        raise HTTPException(500, "Could not write audit record.")


@app.get("/audit")
def audit(limit: int = 50):
    try:
        return {"events": get_audit_history(limit)}
    except ValueError as error:
        raise HTTPException(422, str(error))
