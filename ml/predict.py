"""
ml/predict.py - Inference helper for the bus delay model.

Used by bot.py /api/predict_delay. Loads the model lazily so the
server doesn't pay startup cost if the endpoint is never hit.

Public API:
    load_model() -> bool
    predict_delay(line, operator, hour=None, day_of_week=None,
                  jam_factor=None, expected_kmh=None) -> dict | None
"""
from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger("ml.predict")

ROOT = Path(__file__).resolve().parent
MODEL_PATH = ROOT / "saved" / "delay_model.pkl"
META_PATH = ROOT / "saved" / "feature_meta.json"

# Israel time = UTC+2 (winter) or UTC+3 (summer). Approximate with UTC+3.
ISRAEL_TZ = timezone(timedelta(hours=3))

_model: Any = None
_meta: dict | None = None
_route_expected_kmh: dict[str, float] = {}
_global_expected_kmh: float = 25.0


def load_model() -> bool:
    """Load model + metadata. Returns True on success.

    Idempotent: subsequent calls are no-ops once loaded.
    """
    global _model, _meta, _route_expected_kmh, _global_expected_kmh
    if _model is not None and _meta is not None:
        return True

    if not MODEL_PATH.exists() or not META_PATH.exists():
        logger.warning("delay model not found (need %s + %s)", MODEL_PATH, META_PATH)
        return False

    try:
        import joblib  # local import — bot.py shouldn't pay this cost on startup

        _model = joblib.load(MODEL_PATH)
        _meta = json.loads(META_PATH.read_text(encoding="utf-8"))

        # Build a route -> expected_kmh lookup from a side file if present
        side = ROOT / "saved" / "route_expected_kmh.json"
        if side.exists():
            _route_expected_kmh = json.loads(side.read_text(encoding="utf-8"))
        _global_expected_kmh = float(_meta.get("global_expected_kmh", 25.0))
        logger.info("delay model loaded: %s", _meta.get("model_version"))
        return True
    except Exception as ex:
        logger.exception("failed to load delay model: %s", ex)
        _model = None
        _meta = None
        return False


def _now_israel() -> datetime:
    return datetime.now(timezone.utc).astimezone(ISRAEL_TZ)


def predict_delay(
    line: str | None,
    operator: str | None = None,
    hour: int | None = None,
    day_of_week: int | None = None,
    jam_factor: float | None = None,
    expected_kmh: float | None = None,
) -> dict | None:
    """Predict expected delay in minutes.

    Returns:
        {"predicted_delay_minutes": float,
         "confidence_interval": [low, high],
         "model_version": str,
         "features_used": {...}}
        or None if the model isn't loaded / inputs can't be encoded.
    """
    if not load_model():
        return None
    assert _model is not None and _meta is not None

    now = _now_israel()
    if hour is None:
        hour = now.hour
    if day_of_week is None:
        day_of_week = now.weekday()  # Mon=0 .. Sun=6
    if jam_factor is None:
        jam_factor = 2.0  # neutral default
    is_weekend = 1 if day_of_week in (4, 5) else 0  # Israel Fri/Sat
    is_rush_hour = 1 if hour in (7, 8, 9, 16, 17, 18) else 0

    if expected_kmh is None:
        expected_kmh = _route_expected_kmh.get(str(line), _global_expected_kmh) if line else _global_expected_kmh

    # Bucket route -> "other" if not in known list
    known_routes = set(_meta.get("known_routes", []))
    route_bucket = str(line) if line and str(line) in known_routes else "other"

    # Bucket operator -> "Unknown" if not in known list
    known_operators = set(_meta.get("known_operators", []))
    op = str(operator) if operator and str(operator) in known_operators else "Unknown"

    try:
        import pandas as pd  # local import to keep cold path cheap

        x = pd.DataFrame([{
            "hour_of_day": int(hour),
            "day_of_week": int(day_of_week),
            "is_weekend": int(is_weekend),
            "is_rush_hour": int(is_rush_hour),
            "expected_kmh": float(expected_kmh),
            "avg_jam_factor": float(jam_factor),
            "route_bucket": route_bucket,
            "operator_name": op,
        }])
        # Re-order to match training schema
        x = x[_meta["all_feature_columns"]]
        yhat = float(_model.predict(x)[0])
        yhat = max(0.0, yhat)

        # Confidence band: +/- 1 residual std on the held-out test set
        std = float(_meta.get("residual_std", 1.0))
        ci = [max(0.0, yhat - std), yhat + std]

        return {
            "predicted_delay_minutes": round(yhat, 2),
            "confidence_interval": [round(ci[0], 2), round(ci[1], 2)],
            "model_version": _meta.get("model_version", "unknown"),
            "model_name": _meta.get("model_name", "unknown"),
            "features_used": {
                "line": route_bucket if route_bucket != "other" else f"{line} (bucketed as 'other')",
                "operator": op,
                "hour": int(hour),
                "day_of_week": int(day_of_week),
                "is_weekend": bool(is_weekend),
                "is_rush_hour": bool(is_rush_hour),
                "jam_factor": round(float(jam_factor), 2),
                "expected_kmh": round(float(expected_kmh), 1),
            },
        }
    except Exception as ex:
        logger.exception("predict_delay failed for line=%s op=%s: %s", line, operator, ex)
        return None


if __name__ == "__main__":
    # quick sanity check
    if not load_model():
        print("MODEL NOT LOADED")
        raise SystemExit(1)
    out = predict_delay(line="171", operator="Dan", hour=8, day_of_week=1)
    print(json.dumps(out, ensure_ascii=False, indent=2))
    out = predict_delay(line="89", operator="Egged", hour=14, day_of_week=5)
    print(json.dumps(out, ensure_ascii=False, indent=2))
