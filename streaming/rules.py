"""Pure card-fraud rule functions over a per-transaction context dict.

Each returns a non-negative risk contribution. No I/O, no Spark, no label access.
"""
from streaming.geo import implied_speed_kmh

HIGH_RISK_MCCS = {"7995", "6051"}        # gambling, quasi-cash
VELOCITY_CEILING = 20                     # auths per card per window
CARD_TESTING_MIN = 10                     # small-amount auths in window
SMALL_AMOUNT = 5.0
IMPOSSIBLE_SPEED_KMH = 1000.0             # faster than a commercial flight
AMOUNT_ANOMALY_RATIO = 10.0               # amount vs card average


def velocity_score(ctx: dict) -> float:
    over = ctx.get("card_auth_count", 0) - VELOCITY_CEILING
    return float(min(over, 80)) * 1.0 if over > 0 else 0.0


def card_testing_score(ctx: dict) -> float:
    n = ctx.get("card_small_amt_count", 0)
    if n >= CARD_TESTING_MIN and ctx.get("amount", 0) <= SMALL_AMOUNT:
        return float(n) * 3.0
    return 0.0


def impossible_travel_score(ctx: dict) -> float:
    speed = implied_speed_kmh(ctx.get("distance_km", 0.0),
                              ctx.get("hours_since_prev", 1e9))
    return 60.0 if speed > IMPOSSIBLE_SPEED_KMH else 0.0


def amount_anomaly_score(ctx: dict) -> float:
    avg = ctx.get("card_avg_amount", 0.0)
    if avg > 0 and ctx.get("amount", 0.0) >= AMOUNT_ANOMALY_RATIO * avg:
        return 35.0
    return 0.0


def high_risk_mcc_score(ctx: dict) -> float:
    return 20.0 if ctx.get("mcc") in HIGH_RISK_MCCS else 0.0


def cnp_score(ctx: dict) -> float:
    if not ctx.get("is_card_present", True) and ctx.get("device_is_new", False):
        return 15.0
    return 0.0
