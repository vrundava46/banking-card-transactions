from common.schemas import (TOPICS, AUTH_FIELDS, DECISION_FIELDS,
                            SETTLEMENT_FIELDS, CHARGEBACK_FIELDS)


def test_topics_present():
    assert TOPICS == {"authorizations": "card.authorizations",
                      "decisions": "card.decisions",
                      "settlements": "card.settlements",
                      "chargebacks": "card.chargebacks"}


def test_auth_has_correlation_and_label():
    assert AUTH_FIELDS[0] == "auth_id"
    assert "card_id" in AUTH_FIELDS
    assert "label_is_fraud" in AUTH_FIELDS


def test_all_topics_share_auth_id():
    for fields in (AUTH_FIELDS, DECISION_FIELDS, SETTLEMENT_FIELDS, CHARGEBACK_FIELDS):
        assert "auth_id" in fields
