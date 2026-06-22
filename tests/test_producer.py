import json
from generator.producer import emit_batch


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, key, value):
        self.sent.append((topic, key, value))

    def flush(self):
        pass


def test_emit_batch_produces_all_four_topics():
    fake = FakeProducer()
    emit_batch(fake, n_normal=5, fraud=False)
    topics = {t for (t, _, _) in fake.sent}
    assert "card.authorizations" in topics
    assert "card.decisions" in topics
    assert "card.settlements" in topics


def test_fraud_batch_emits_labeled_card_testing():
    fake = FakeProducer()
    emit_batch(fake, n_normal=0, fraud=True)
    auths = [json.loads(v) for (t, _, v) in fake.sent if t == "card.authorizations"]
    assert auths and all(a["label_is_fraud"] for a in auths)
    chargebacks = [t for (t, _, _) in fake.sent if t == "card.chargebacks"]
    assert chargebacks  # fraud produces chargebacks
