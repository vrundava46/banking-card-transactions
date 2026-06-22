select
  decision,
  decline_reason,
  count(*) as txn_count,
  round(1.0 * count_if(decision = 'approve') / nullif(count(*), 0), 4) as approval_rate
from iceberg.silver.transactions
group by 1, 2
