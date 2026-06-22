from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG("card_model_scoring", start_date=datetime(2026, 1, 1),
         schedule="@hourly", catchup=False) as dag:
    BashOperator(task_id="score_transactions", bash_command="python -m batch.run_score")
