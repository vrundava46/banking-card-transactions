import random
import uuid
from datetime import datetime, timezone

ENTRY_MODES = ["chip", "contactless", "online", "swipe"]
MCCS = ["5411", "5812", "5999", "4111", "5732", "7995"]  # 7995 = gambling (risky)
HIGH_RISK_MCCS = {"7995", "6051"}
MERCHANTS = [f"mer_{i}" for i in range(1, 31)]
CARDS = [f"card_{i}" for i in range(1, 51)]
# (country, lat, lon)
GEOS = [("US", 40.7, -74.0), ("GB", 51.5, -0.13), ("DE", 52.5, 13.4),
        ("IN", 19.1, 72.9), ("BR", -23.5, -46.6)]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_auth_event(card_id=None, amount=None, mcc=None, entry_mode=None,
                    is_card_present=None, geo=None, label=False) -> dict:
    g = geo or random.choice(GEOS)
    em = entry_mode or random.choice(ENTRY_MODES)
    return {
        "auth_id": str(uuid.uuid4()),
        "card_id": card_id or random.choice(CARDS),
        "account_id": "acct_" + (card_id or random.choice(CARDS)).split("_")[-1],
        "merchant_id": random.choice(MERCHANTS),
        "mcc": mcc or random.choice(MCCS),
        "merchant_country": g[0],
        "amount": round(amount if amount is not None else random.uniform(5, 300), 2),
        "currency": "USD",
        "entry_mode": em,
        "is_card_present": is_card_present if is_card_present is not None
                           else em in ("chip", "contactless", "swipe"),
        "lat": g[1],
        "lon": g[2],
        "device_id": f"dev_{random.randint(1, 200)}",
        "event_ts": _now_iso(),
        "label_is_fraud": bool(label),
    }


def normal_traffic(count: int) -> list[dict]:
    return [make_auth_event(label=False) for _ in range(count)]


def card_testing_burst(count: int, card_id: str = "card_bad") -> list[dict]:
    """Rapid micro-amount CNP auths on one card — the card-testing signature."""
    return [make_auth_event(card_id=card_id, amount=round(random.uniform(0.5, 4.5), 2),
                            entry_mode="online", is_card_present=False, label=True)
            for _ in range(count)]


def high_amount_fraud(card_id: str = "card_bad") -> dict:
    return make_auth_event(card_id=card_id, amount=round(random.uniform(2001, 9000), 2),
                           entry_mode="online", is_card_present=False,
                           mcc="7995", label=True)


def make_decision_event(auth: dict, decision: str, risk_score: float,
                        decline_reason: str = "") -> dict:
    return {
        "auth_id": auth["auth_id"],
        "decision": decision,
        "decline_reason": decline_reason,
        "risk_score": float(risk_score),
        "event_ts": _now_iso(),
    }


def make_settlement_event(auth: dict) -> dict:
    return {
        "auth_id": auth["auth_id"],
        "settled_amount": auth["amount"],
        "event_ts": _now_iso(),
    }


def make_chargeback_event(auth: dict) -> dict:
    return {
        "auth_id": auth["auth_id"],
        "dispute_reason": "fraud" if auth["label_is_fraud"] else "service",
        "chargeback_amount": auth["amount"],
        "is_fraud_dispute": bool(auth["label_is_fraud"]),
        "event_ts": _now_iso(),
    }
