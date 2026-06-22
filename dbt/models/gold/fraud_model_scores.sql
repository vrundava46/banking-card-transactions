select
  case when model_prob >= 0.8 then 'high'
       when model_prob >= 0.5 then 'medium' else 'low' end as risk_band,
  count(*) as txn_count,
  avg(model_prob) as avg_prob
from postgres.public.fraud_scores
group by 1
