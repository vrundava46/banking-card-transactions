import json
import random
from generator import scenarios
from common.schemas import TOPICS


def _send(producer, topic, event):
    producer.produce(topic, key=event["auth_id"], value=json.dumps(event))


def emit_batch(producer, n_normal: int, fraud: bool) -> None:
    auths = scenarios.normal_traffic(n_normal)
    if fraud:
        auths = scenarios.card_testing_burst(40) + [scenarios.high_amount_fraud()]
    for a in auths:
        _send(producer, TOPICS["authorizations"], a)
        if a["label_is_fraud"]:
            _send(producer, TOPICS["decisions"],
                  scenarios.make_decision_event(a, "decline", 95.0, "fraud_rule"))
            _send(producer, TOPICS["chargebacks"], scenarios.make_chargeback_event(a))
        else:
            _send(producer, TOPICS["decisions"],
                  scenarios.make_decision_event(a, "approve", 5.0))
            _send(producer, TOPICS["settlements"], scenarios.make_settlement_event(a))
    producer.flush()


def build_kafka_producer(bootstrap: str):
    from confluent_kafka import Producer
    return Producer({"bootstrap.servers": bootstrap})


def main():  # pragma: no cover
    import time
    from common.config import Settings
    s = Settings.from_env()
    p = build_kafka_producer(s.kafka_bootstrap)
    while True:
        emit_batch(p, n_normal=200, fraud=False)
        if random.random() < 0.15:
            emit_batch(p, n_normal=0, fraud=True)
        time.sleep(1)


if __name__ == "__main__":  # pragma: no cover
    main()
