import numpy as np
import pandas as pd
from batch.model import train_model, score_frame, save_model, load_model


def _labeled_features(n=400, seed=0):
    rng = np.random.default_rng(seed)
    # fraud: low log_amount, online, high-risk mcc; legit: opposite. Separable.
    rows = []
    for _ in range(n):
        fraud = rng.random() < 0.3
        rows.append(dict(
            log_amount=rng.normal(0.5 if fraud else 4.5, 0.4),
            is_card_present=0 if fraud else 1,
            is_online=1 if fraud else 0,
            is_high_risk_mcc=1 if fraud and rng.random() < 0.7 else 0,
            hour=int(rng.integers(0, 24)),
            card_txn_rank=int(rng.integers(0, 30)) if fraud else int(rng.integers(0, 3)),
            label=int(fraud)))
    return pd.DataFrame(rows)


def test_train_model_separates_fraud():
    feats = _labeled_features()
    model, metrics = train_model(feats, seed=42)
    assert metrics["roc_auc"] > 0.9          # clearly separable synthetic data
    assert 0 < metrics["n_test"] <= len(feats)


def test_score_frame_high_prob_for_obvious_fraud(tmp_path):
    feats = _labeled_features()
    model, _ = train_model(feats, seed=42)
    obvious = pd.DataFrame([dict(log_amount=0.3, is_card_present=0, is_online=1,
                                 is_high_risk_mcc=1, hour=3, card_txn_rank=25)])
    probs = score_frame(model, obvious)
    assert probs[0] > 0.5


def test_save_and_load_roundtrip(tmp_path):
    feats = _labeled_features()
    model, _ = train_model(feats, seed=42)
    path = tmp_path / "m.joblib"
    save_model(model, str(path))
    loaded = load_model(str(path))
    legit = pd.DataFrame([dict(log_amount=4.6, is_card_present=1, is_online=0,
                               is_high_risk_mcc=0, hour=12, card_txn_rank=1)])
    assert score_frame(loaded, legit)[0] < 0.5
