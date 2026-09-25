"""Month-end: build the client statement for the month that just closed.

Runs on the 1st at 02:00 UTC, after the last hourly run of the month. It runs
all checks again and the full upstream build + tests, so a statement is only
produced when every check on its inputs passes.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from gtdp.alerts import airflow_failure_callback

with DAG(
    dag_id="month_end_statement",
    start_date=datetime(2026, 9, 1),
    schedule="0 2 1 * *",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "on_failure_callback": airflow_failure_callback,
    },
    tags=["gtdp", "compliance"],
) as dag:
    checks = BashOperator(task_id="run_all_checks", bash_command="python -m gtdp check all")

    build_statement = BashOperator(
        task_id="build_statement",
        bash_command=(
            "cd /opt/gtdp/dbt && dbt deps --quiet && "
            "dbt build --select +rpt_client_monthly_statement"
        ),
    )

    checks >> build_statement
