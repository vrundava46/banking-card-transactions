import os
import joblib
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
from batch.features import FEATURE_COLS


def train_model(features: pd.DataFrame, seed: int = 42):
    X = features[FEATURE_COLS]
    y = features["label"].astype(int)
    X_tr, X_te, y_tr, y_te = train_test_split(
        X, y, test_size=0.25, random_state=seed, stratify=y)
    model = HistGradientBoostingClassifier(random_state=seed, max_iter=200)
    model.fit(X_tr, y_tr)
    proba = model.predict_proba(X_te)[:, 1]
    metrics = {"roc_auc": float(roc_auc_score(y_te, proba)), "n_test": int(len(y_te))}
    return model, metrics


def score_frame(model, features: pd.DataFrame):
    return model.predict_proba(features[FEATURE_COLS])[:, 1]


def save_model(model, path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    joblib.dump(model, path)


def load_model(path: str):
    return joblib.load(path)
