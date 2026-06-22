from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG("card_model_training", start_date=datetime(2026, 1, 1),
         schedule="@daily", catchup=False) as dag:
    BashOperator(task_id="train_model", bash_command="python -m batch.run_train")
