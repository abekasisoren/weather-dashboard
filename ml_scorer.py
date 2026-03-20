"""
ml_scorer.py — ML-based trade outcome scorer.

Trains on historical recommendations with CONFIRMED T+3 outcomes (not same-day
noise), using rich weather features captured at recommendation time.

Features:
    Region, Anomaly, Trade direction, Conviction (categorical)
    _trend_dir: worsening / new / stable / recovering (categorical)
    Score: composite final trade score (numeric)
    _sigma: Z-score / statistical extremity of the event
    _seasonality: how surprising the event is for this time of year (3=expected, 9=off-season)
    _confluence: multi-region bonus (0, 0.5, 1.0)
    _pheno_mult: crop-stage sensitivity at time of signal

Label:
    1 = T+3 P&L > 0 (win at 3 business days), 0 = loss/flat
    Only rows with actual T+3 snapshot are used — never same-day fallback.

Model persistence:
    Primary: PostgreSQL ml_model_store table (survives Render deploys)
    Fallback: weather_ml_model.pkl on local filesystem

Usage:
    from ml_scorer import train_model, load_model, predict_win_prob, get_labeled_data
"""

from __future__ import annotations

import os
import pickle
from typing import Optional

import numpy as np
import pandas as pd

DATABASE_URL = os.environ.get("DATABASE_URL")
MODEL_PATH   = os.path.join(os.path.dirname(__file__), "weather_ml_model.pkl")
MIN_SAMPLES  = 10  # minimum T+3-labeled trades before training is allowed

# ─── Feature definition ───────────────────────────────────────────────────────

CATEGORICAL_COLS = ["Region", "Anomaly", "Trade", "Conviction", "_trend_dir"]

NUMERIC_COLS = [
    "Score",         # final composite trade score
    "_sigma",        # statistical extremity (Z-score proxy)
    "_seasonality",  # surprise factor: 3 = peak season, 9 = off-season anomaly
    "_confluence",   # multi-region event bonus (0 / 0.5 / 1.0)
    "_pheno_mult",   # crop-stage sensitivity at signal time
]


# ─── DB model store ───────────────────────────────────────────────────────────

_ML_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ml_model_store (
    id          SERIAL PRIMARY KEY,
    saved_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    model_name  TEXT,
    n_trained   INT,
    accuracy    FLOAT,
    model_blob  BYTEA NOT NULL
);
"""


def _ensure_model_table() -> None:
    import psycopg
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(_ML_TABLE_SQL)
        conn.commit()


def _save_model_to_db(payload: dict, accuracy: float) -> None:
    """Serialize model payload to PostgreSQL.  No-op if DATABASE_URL unset."""
    if not DATABASE_URL:
        return
    try:
        _ensure_model_table()
        blob = pickle.dumps(payload)
        import psycopg
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO ml_model_store (model_name, n_trained, accuracy, model_blob) "
                    "VALUES (%s, %s, %s, %s)",
                    (payload.get("model_name"), payload.get("n_trained"), accuracy, blob),
                )
            conn.commit()
    except Exception:
        pass  # fall through to pkl fallback below


def _load_model_from_db() -> Optional[dict]:
    """Load most-recently saved model from PostgreSQL.  Returns None on failure."""
    if not DATABASE_URL:
        return None
    try:
        import psycopg
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT model_blob FROM ml_model_store ORDER BY id DESC LIMIT 1"
                )
                row = cur.fetchone()
        if row:
            return pickle.loads(bytes(row[0]))
    except Exception:
        pass
    return None


# ─── Data prep ────────────────────────────────────────────────────────────────

def get_labeled_data(aftermath_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter aftermath_df to rows with a confirmed T+3 snapshot.
    Never uses same-day P&L — only clean 3-business-day outcomes.
    Adds 'label': 1 = win (T+3 P&L > 0), 0 = loss / flat.
    """
    if aftermath_df.empty or "_pnl_t3_raw" not in aftermath_df.columns:
        return pd.DataFrame()
    labeled = aftermath_df[aftermath_df["_pnl_t3_raw"].notna()].copy()
    if labeled.empty:
        return pd.DataFrame()
    labeled["label"] = (labeled["_pnl_t3_raw"] > 0).astype(int)
    return labeled.reset_index(drop=True)


def _build_feature_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """
    One-hot encode categoricals + keep numerics.
    Missing numeric columns are filled with sensible defaults.
    Returns aligned DataFrame with consistent column set.
    """
    # Fill defaults for columns that may not exist in older logged rows
    defaults = {"_sigma": 1.0, "_seasonality": 5.0, "_confluence": 0.0, "_pheno_mult": 1.0}
    for col, default in defaults.items():
        if col not in df.columns:
            df = df.copy()
            df[col] = default

    available_cats = [c for c in CATEGORICAL_COLS if c in df.columns]
    available_nums = [c for c in NUMERIC_COLS      if c in df.columns]

    encoded = pd.get_dummies(df[available_cats], drop_first=False)
    numeric = df[available_nums].fillna(0).reset_index(drop=True)
    encoded = encoded.reset_index(drop=True)
    return pd.concat([encoded, numeric], axis=1)


# ─── Training ─────────────────────────────────────────────────────────────────

def train_model(aftermath_df: pd.DataFrame) -> dict:
    """
    Train classifier on T+3-labeled trade outcomes.

    Returns metrics dict:
        n_samples, accuracy, accuracy_std, wins, losses,
        top_features [(name, importance)...], model_name, error (on failure)

    Saves model to PostgreSQL (primary) and local pkl (fallback).
    """
    labeled = get_labeled_data(aftermath_df)
    n = len(labeled)

    if n < MIN_SAMPLES:
        return {
            "error": (
                f"Need at least {MIN_SAMPLES} trades with confirmed T+3 outcomes — "
                f"only {n} available so far.  Keep fetching quotes daily and come back."
            )
        }

    X = _build_feature_matrix(labeled)
    y = labeled["label"].values
    feature_cols = list(X.columns)

    wins   = int(y.sum())
    losses = int(n - wins)

    # Prefer XGBoost, fall back to sklearn GradientBoosting
    try:
        from xgboost import XGBClassifier
        clf = XGBClassifier(
            n_estimators=200,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_weight=2,
            eval_metric="logloss",
            random_state=42,
        )
        model_name = "XGBoost"
    except ImportError:
        from sklearn.ensemble import GradientBoostingClassifier
        clf = GradientBoostingClassifier(
            n_estimators=200,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            random_state=42,
        )
        model_name = "GradientBoosting (sklearn)"

    # Cross-validated accuracy (k = min 5, max n//2)
    from sklearn.model_selection import cross_val_score
    k = max(2, min(5, n // 2))
    cv_scores = cross_val_score(clf, X, y, cv=k, scoring="accuracy")

    # Final fit on all data
    clf.fit(X, y)

    # Feature importance
    importances     = clf.feature_importances_
    feat_importance = sorted(
        zip(feature_cols, importances.tolist()), key=lambda x: x[1], reverse=True
    )

    accuracy = round(float(cv_scores.mean()), 3)

    payload = {
        "model":        clf,
        "model_name":   model_name,
        "feature_cols": feature_cols,
        "n_trained":    n,
    }

    # Persist — DB first, local pkl as fallback
    _save_model_to_db(payload, accuracy)
    try:
        with open(MODEL_PATH, "wb") as f:
            pickle.dump(payload, f)
    except Exception:
        pass

    return {
        "n_samples":     n,
        "wins":          wins,
        "losses":        losses,
        "accuracy":      accuracy,
        "accuracy_std":  round(float(cv_scores.std()), 3),
        "model_name":    model_name,
        "top_features":  feat_importance[:12],
    }


# ─── Inference ────────────────────────────────────────────────────────────────

def load_model() -> Optional[dict]:
    """
    Load saved model. DB takes priority (survives deploys); local pkl is fallback.
    Returns None if model hasn't been trained yet.
    """
    payload = _load_model_from_db()
    if payload is not None:
        return payload
    # Fallback: local file
    if os.path.exists(MODEL_PATH):
        try:
            with open(MODEL_PATH, "rb") as f:
                return pickle.load(f)
        except Exception:
            pass
    return None


def predict_win_prob(signals_df: pd.DataFrame) -> Optional[pd.Series]:
    """
    Predict win probability (0–1) for each row in signals_df.
    signals_df must have columns matching CATEGORICAL_COLS + NUMERIC_COLS.
    Returns None if no model is loaded.
    """
    payload = load_model()
    if payload is None:
        return None

    clf          = payload["model"]
    feature_cols = payload["feature_cols"]

    X = _build_feature_matrix(signals_df)

    # Align to training feature space
    for col in feature_cols:
        if col not in X.columns:
            X[col] = 0
    X = X[feature_cols]

    try:
        probs = clf.predict_proba(X)[:, 1]
        return pd.Series(probs, index=signals_df.index).round(3)
    except Exception:
        return None


def model_info() -> dict:
    """Return summary of the currently saved model (for dashboard display)."""
    payload = load_model()
    if payload is None:
        return {"trained": False}
    return {
        "trained":       True,
        "model_name":    payload.get("model_name", "Unknown"),
        "n_trained":     payload.get("n_trained", "?"),
        "feature_cols":  payload.get("feature_cols", []),
    }
