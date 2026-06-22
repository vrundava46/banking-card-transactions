from streaming import rules


def ctx(**kw):
    base = dict(amount=50.0, mcc="5411", is_card_present=True,
                card_auth_count=2, card_small_amt_count=0, card_avg_amount=60.0,
                distance_km=1.0, hours_since_prev=5.0, device_is_new=False)
    base.update(kw)
    return base


def test_velocity():
    assert rules.velocity_score(ctx(card_auth_count=40)) > 0
    assert rules.velocity_score(ctx(card_auth_count=2)) == 0


def test_card_testing():
    hot = ctx(card_small_amt_count=25, amount=1.0)
    assert rules.card_testing_score(hot) > 0
    assert rules.card_testing_score(ctx(card_small_amt_count=0)) == 0


def test_impossible_travel():
    # 5000 km in 0.2 h -> 25000 km/h, impossible
    assert rules.impossible_travel_score(ctx(distance_km=5000, hours_since_prev=0.2)) > 0
    assert rules.impossible_travel_score(ctx(distance_km=10, hours_since_prev=5)) == 0


def test_amount_anomaly():
    assert rules.amount_anomaly_score(ctx(amount=5000, card_avg_amount=50)) > 0
    assert rules.amount_anomaly_score(ctx(amount=55, card_avg_amount=50)) == 0


def test_high_risk_mcc():
    assert rules.high_risk_mcc_score(ctx(mcc="7995")) > 0
    assert rules.high_risk_mcc_score(ctx(mcc="5411")) == 0


def test_cnp_new_device():
    assert rules.cnp_score(ctx(is_card_present=False, device_is_new=True)) > 0
    assert rules.cnp_score(ctx(is_card_present=True, device_is_new=False)) == 0
