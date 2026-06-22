select
  merchant_country,
  count_if(decision = 'decline') as declined_count,
  sum(case when decision = 'decline' then amount else 0 end) as blocked_amount,
  sum(case when is_fraud_dispute then chargeback_amount else 0 end) as fraud_loss
from iceberg.silver.transactions
group by 1
