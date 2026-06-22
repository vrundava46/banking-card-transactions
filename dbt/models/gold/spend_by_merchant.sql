select
  merchant_id,
  count(*) as txn_count,
  sum(amount) as total_amount
from iceberg.silver.transactions
group by 1
