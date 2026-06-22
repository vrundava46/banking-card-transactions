from streaming.scoring import score_transaction, severity, decision


def ctx(**kw):
    base = dict(amount=50.0, mcc="5411", is_card_present=True,
                card_auth_count=2, card_small_amt_count=0, card_avg_amount=60.0,
                distance_km=1.0, hours_since_prev=5.0, device_is_new=False)
    base.update(kw)
    return base


def test_clean_txn_low_and_approved():
    s = score_transaction(ctx())
    assert s == 0
    assert severity(s) == "low"
    assert decision(s) == "approve"


def test_card_testing_high_and_declined():
    s = score_transaction(ctx(card_small_amt_count=25, amount=1.0,
                              is_card_present=False, device_is_new=True))
    assert s >= 50
    assert severity(s) == "high"
    assert decision(s) == "decline"


def test_decision_threshold():
    assert decision(0) == "approve"
    assert decision(49) == "approve"
    assert decision(50) == "decline"
