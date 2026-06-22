from streaming.fraud_job import apply_scoring


def test_apply_scoring_combines_rules_and_model():
    rows = [
        dict(auth_id="a1", card_id="c_bad", amount=1.0, mcc="7995",
             is_card_present=False, card_auth_count=40, card_small_amt_count=25,
             card_avg_amount=50.0, distance_km=1.0, hours_since_prev=5.0,
             device_is_new=True),
        dict(auth_id="a2", card_id="c_ok", amount=60.0, mcc="5411",
             is_card_present=True, card_auth_count=2, card_small_amt_count=0,
             card_avg_amount=60.0, distance_km=1.0, hours_since_prev=5.0,
             device_is_new=False),
    ]
    # stub model: returns high prob for the small-amount online row
    def model_prob_fn(r):
        return 0.95 if r["amount"] < 5 else 0.02
    out = {o["auth_id"]: o for o in apply_scoring(rows, model_prob_fn)}
    assert out["a1"]["decision"] == "decline"
    assert out["a1"]["severity"] == "high"
    assert out["a1"]["model_prob"] == 0.95
    assert out["a2"]["decision"] == "approve"
    assert out["a2"]["model_prob"] == 0.02
