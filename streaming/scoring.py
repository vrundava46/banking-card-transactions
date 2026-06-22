from streaming import rules

RULES = [
    rules.velocity_score,
    rules.card_testing_score,
    rules.impossible_travel_score,
    rules.amount_anomaly_score,
    rules.high_risk_mcc_score,
    rules.cnp_score,
]
DECLINE_THRESHOLD = 50.0


def score_transaction(ctx: dict) -> float:
    return float(sum(rule(ctx) for rule in RULES))


def severity(score: float) -> str:
    if score >= 50:
        return "high"
    if score >= 20:
        return "medium"
    return "low"


def decision(score: float) -> str:
    return "decline" if score >= DECLINE_THRESHOLD else "approve"
