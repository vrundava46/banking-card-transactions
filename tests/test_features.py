import pandas as pd
from batch.features import build_features, FEATURE_COLS


def test_build_features_columns_and_label():
    df = pd.DataFrame([
        dict(amount=1.0, mcc="7995", entry_mode="online", is_card_present=False,
             event_ts="2026-06-21T10:00:00+00:00", card_id="c1", label_is_fraud=True),
        dict(amount=80.0, mcc="5411", entry_mode="chip", is_card_present=True,
             event_ts="2026-06-21T14:00:00+00:00", card_id="c2", label_is_fraud=False),
    ])
    out = build_features(df)
    for c in FEATURE_COLS:
        assert c in out.columns
    assert "label" in out.columns
    assert out.loc[0, "is_high_risk_mcc"] == 1
    assert out.loc[1, "is_high_risk_mcc"] == 0
    assert out.loc[0, "log_amount"] < out.loc[1, "log_amount"]
    assert out.loc[0, "hour"] == 10
