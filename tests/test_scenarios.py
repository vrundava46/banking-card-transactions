from generator.scenarios import (make_auth_event, normal_traffic,
                                  card_testing_burst, high_amount_fraud,
                                  make_decision_event, make_settlement_event,
                                  make_chargeback_event)


def test_make_auth_event_has_all_fields():
    e = make_auth_event()
    for k in ["auth_id", "card_id", "merchant_id", "mcc", "amount",
              "entry_mode", "is_card_present", "lat", "lon", "event_ts",
              "label_is_fraud"]:
        assert k in e


def test_normal_traffic_mostly_legit():
    events = normal_traffic(200)
    fraud = [e for e in events if e["label_is_fraud"]]
    assert len(fraud) == 0
    assert len({e["card_id"] for e in events}) > 20  # many distinct cards


def test_card_testing_burst_is_labeled_fraud_small_amounts():
    events = card_testing_burst(40, card_id="card_bad")
    assert all(e["label_is_fraud"] for e in events)
    assert all(e["card_id"] == "card_bad" for e in events)
    assert all(e["amount"] < 5.0 for e in events)   # micro-amounts
    assert all(not e["is_card_present"] for e in events)  # CNP


def test_high_amount_fraud_labeled():
    e = high_amount_fraud(card_id="card_bad")
    assert e["label_is_fraud"] is True
    assert e["amount"] > 2000


def test_decision_links_to_auth():
    a = make_auth_event()
    d = make_decision_event(a, decision="approve", risk_score=12.0)
    assert d["auth_id"] == a["auth_id"]
    assert d["decision"] == "approve"
    assert d["risk_score"] == 12.0


def test_settlement_uses_amount():
    a = make_auth_event(amount=42.0)
    s = make_settlement_event(a)
    assert s["auth_id"] == a["auth_id"]
    assert s["settled_amount"] == 42.0


def test_chargeback_marks_fraud_dispute():
    a = make_auth_event(label=True)
    c = make_chargeback_event(a)
    assert c["auth_id"] == a["auth_id"]
    assert c["is_fraud_dispute"] is True
    assert c["chargeback_amount"] == a["amount"]
