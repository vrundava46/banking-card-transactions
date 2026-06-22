TOPICS = {
    "authorizations": "card.authorizations",
    "decisions": "card.decisions",
    "settlements": "card.settlements",
    "chargebacks": "card.chargebacks",
}
ALERTS_TOPIC = "card.alerts"

AUTH_FIELDS = [
    "auth_id", "card_id", "account_id", "merchant_id", "mcc", "merchant_country",
    "amount", "currency", "entry_mode", "is_card_present", "lat", "lon",
    "device_id", "event_ts", "label_is_fraud",
]
DECISION_FIELDS = ["auth_id", "decision", "decline_reason", "risk_score", "event_ts"]
SETTLEMENT_FIELDS = ["auth_id", "settled_amount", "event_ts"]
CHARGEBACK_FIELDS = ["auth_id", "dispute_reason", "chargeback_amount",
                     "is_fraud_dispute", "event_ts"]
