"""
ml_scorer.py — ML-based trade outcome scorer.

Trains on historical recommendations with known P&L outcomes (from the
Aftermath tab), then predicts win probability for new signals.

Features used at training time:
    region, anomaly, trade direction, conviction, final_trade_score

Label: 1 = P&L > 0 (win), 0 = P&L <= 0 (loss)

Model:
    XGBoost if installed, otherwise sklearn GradientBoostingClassifier.
    Saved/loaded from weather_ml_model.pkl in the project directory.

Usage:
    from ml_scorer import train_model, load_model, predict_win_prob, get_labeled_data
"""

from __future__ import annotations

import os
import pickle
from typing import Optional

import numpy as np
import pandas as pd

MODEL_PATH = os.path.join(os.path.dirname(__file__), "weather_ml_model.pkl")
MIN_SAMPLES = 10  # minimum labeled trades before training is allowed

# Columns used as features — must exist in aftermath_df
CATEGORICAL_COLS = ["Region", "Anomaly", "Trade", "Conviction"]
NUMERIC_COLS = ["Score"]


# ─── Data prep ────────────────────────────────────────────────────────────────

def get_labeled_data(aftermath_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter aftermath_df to rows with computable P&L (entry price was fetched).
    Adds a 'label' column: 1 = win (P&L > 0), 0 = loss/flat.
    """
    if aftermath_df.empty:
        return pd.DataFrame()
    labeled = aftermath_df[aftermath_df["_pnl_raw"].notna()].copy()
    labeled["label"] = (labeled["_pnl_raw"] > 0).astype(int)
    return labeled.reset_index(drop=True)


def _build_feature_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """One-hot encode categoricals + keep numerics. Returns aligned DataFrame."""
    available_cats = [c for c in CATEGORICAL_COLS if c in df.columns]
    available_nums = [c for c in NUMERIC_COLS if c in df.columns]

    encoded = pd.get_dummies(df[available_cats], drop_first=False)
    numeric = df[available_nums].fillna(0).reset_index(drop=True)
    encoded = encoded.reset_index(drop=True)
    return pd.concat([encoded, numeric], axis=1)


# ─── Training ─────────────────────────────────────────────────────────────────

def train_model(aftermath_df: pd.DataFrame) -> dict:
    """
    Train a classifier on labeled trade outcomes from aftermath_df.

    Returns a metrics dict:
        n_samples, accuracy, accuracy_std, wins, losses,
        top_features [(name, importance), ...], error (if failed)

    Saves the fitted model + feature columns to MODEL_PATH.
    """
    labeled = get_labeled_data(aftermath_df)
    n = len(labeled)

    if n < MIN_SAMPLES:
        return {"error": f"Need at least {MIN_SAMPLES} labeled trades — only {n} available so far."}

    X = _build_feature_matrix(labeled)
    y = labeled["label"].values
    feature_cols = list(X.columns)

    # Prefer XGBoost, fall back to sklearn GradientBoosting
    try:
        from xgboost import XGBClassifier
        clf = XGBClassifier(
            n_estimators=150,
            max_depth=3,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            use_label_encoder=False,
            eval_metric="logloss",
            random_state=42,
        )
        model_name = "XGBoost"
    except ImportError:
        from sklearn.ensemble import GradientBoostingClassifier
        clf = GradientBoostingClassifier(
            n_estimators=150,
            max_depth=3,
            learning_rate=0.1,
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
    importances = clf.feature_importances_
    feat_importance = sorted(
        zip(feature_cols, importances), key=lambda x: x[1], reverse=True
    )

    # Persist
    payload = {
        "model": clf,
        "model_name": model_name,
        "feature_cols": feature_cols,
        "n_trained": n,
    }
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(payload, f)

    return {
        "n_samples": n,
        "wins": int(y.sum()),
        "losses": int(n - y.sum()),
        "accuracy": round(float(cv_scores.mean()), 3),
        "accuracy_std": round(float(cv_scores.std()), 3),
        "model_name": model_name,
        "top_features": feat_importance[:12],
    }


# ─── Inference ────────────────────────────────────────────────────────────────

def load_model() -> Optional[dict]:
    """Load saved model payload. Returns None if model hasn't been trained yet."""
    if not os.path.exists(MODEL_PATH):
        return None
    try:
        with open(MODEL_PATH, "rb") as f:
            return pickle.load(f)
    except Exception:
        return None


def predict_win_prob(signals_df: pd.DataFrame) -> Optional[pd.Series]:
    """
    Predict win probability (0-1) for each row in signals_df.
    signals_df must have columns matching CATEGORICAL_COLS + NUMERIC_COLS.
    Returns None if no model is loaded.
    """
    payload = load_model()
    if payload is None:
        return None

    clf = payload["model"]
    feature_cols = payload["feature_cols"]

    X = _build_feature_matrix(signals_df)

    # Align to training feature space (add missing cols as 0, drop extra)
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
        "trained": True,
        "model_name": payload.get("model_name", "Unknown"),
        "n_trained": payload.get("n_trained", "?"),
        "feature_cols": payload.get("feature_cols", []),
    }
