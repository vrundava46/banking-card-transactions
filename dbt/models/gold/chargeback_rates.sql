select
  mcc,
  count(*) as txn_count,
  count_if(is_charged_back) as chargebacks,
  count_if(is_fraud_dispute) as fraud_chargebacks,
  round(1.0 * count_if(is_charged_back) / nullif(count(*), 0), 4) as chargeback_rate
from iceberg.silver.transactions
group by 1
