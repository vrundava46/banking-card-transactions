select
  mcc,
  count(*) as txn_count,
  sum(amount) as total_amount,
  avg(amount) as avg_amount
from iceberg.silver.transactions
group by 1
