import numpy as np
import pandas as pd

HIGH_RISK_MCCS = {"7995", "6051"}
FEATURE_COLS = ["log_amount", "is_card_present", "is_online", "is_high_risk_mcc",
                "hour", "card_txn_rank"]


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    out["log_amount"] = np.log1p(df["amount"].astype(float))
    out["is_card_present"] = df["is_card_present"].astype(int)
    out["is_online"] = (df["entry_mode"] == "online").astype(int)
    out["is_high_risk_mcc"] = df["mcc"].isin(HIGH_RISK_MCCS).astype(int)
    out["hour"] = pd.to_datetime(df["event_ts"]).dt.hour
    # simple per-card velocity proxy: ordinal rank of the txn within its card
    out["card_txn_rank"] = df.groupby("card_id").cumcount()
    if "label_is_fraud" in df.columns:
        out["label"] = df["label_is_fraud"].astype(int)
    return out
