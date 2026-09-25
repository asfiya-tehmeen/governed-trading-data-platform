"""Hourly: prove the raw data is trustworthy, then build the warehouse.

schema drift -> freshness / completeness / duplicates / volume -> dbt build

Schema drift runs first and alone: if a source table no longer matches its
contract, nothing else should read it. Any FAIL stops the run before dbt,
so the marts keep their last good state instead of being rebuilt on bad data.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from gtdp.alerts import airflow_failure_callback

DBT = "cd /opt/gtdp/dbt && dbt"


with DAG(
    dag_id="trading_platform_hourly",
    start_date=datetime(2026, 9, 1),
    schedule="5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": airflow_failure_callback,
    },
    tags=["gtdp", "quality"],
) as dag:
    schema_drift = BashOperator(
        task_id="check_schema_drift",
        bash_command="python -m gtdp check schema-drift",
        retries=0,  # drift won't fix itself on retry
    )

    raw_checks = [
        BashOperator(task_id=f"check_{group}", bash_command=f"python -m gtdp check {group}")
        for group in ("freshness", "completeness", "duplicates", "volume")
    ]

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} deps --quiet && {DBT} build --exclude rpt_client_monthly_statement",
    )

    schema_drift >> raw_checks >> dbt_build
