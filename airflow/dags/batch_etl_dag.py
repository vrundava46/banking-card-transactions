from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG("card_batch_etl", start_date=datetime(2026, 1, 1),
         schedule="@hourly", catchup=False) as dag:
    silver = BashOperator(task_id="bronze_to_silver",
                          bash_command="python -m batch.run_silver")
    dbt = BashOperator(task_id="dbt_build",
                       bash_command="cd $PROJECT_ROOT/dbt && dbt build --profiles-dir .")
    quality = BashOperator(task_id="quality_checks",
                           bash_command="python -m quality.expectations_silver")
    silver >> dbt >> quality
