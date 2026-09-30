"""Batch layer orchestration: one run per simulated day."""
from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

from batch import report, tasks

SIM_DAY_MINUTES = int(os.environ.get("SIM_DAY_SECONDS", "300")) // 60


def on_failure(context) -> None:
    """Write the failure to the audit trail; Prometheus alerts on stale reports."""
    ti = context["ti"]
    sim_date = ti.xcom_pull(task_ids="resolve_sim_date")
    tasks.record_failure(context["run_id"], ti.task_id, sim_date, str(context.get("exception")))


@dag(
    dag_id="grid_daily_billing",
    description="Daily billing and solar-contribution report (batch layer of the Lambda architecture)",
    schedule=f"*/{SIM_DAY_MINUTES} * * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "grid-data-eng",
        "retries": 2,
        "retry_delay": timedelta(seconds=20),
        "on_failure_callback": on_failure,
    },
    params={"sim_date": Param(None, type=["null", "string"], description="Reprocess this simulated day")},
    tags=["smart-grid", "batch-layer"],
)
def grid_daily_billing():
    @task
    def resolve_sim_date(data_interval_end=None, params=None) -> str:
        try:
            return tasks.resolve_sim_date(data_interval_end, params.get("sim_date"))
        except tasks.NoCompletedDay as exc:
            raise AirflowSkipException(str(exc)) from exc

    @task.sensor(poke_interval=5, timeout=180, mode="poke")
    def wait_for_tariff_file(sim_date: str) -> bool:
        return tasks.tariff_file_ready(sim_date)

    @task.sensor(poke_interval=5, timeout=300, mode="poke")
    def wait_for_late_readings(sim_date: str) -> bool:
        return tasks.late_data_grace_elapsed(sim_date)

    @task
    def load_tariffs(sim_date: str, run_id=None) -> dict:
        return tasks.load_tariffs(sim_date, run_id)

    @task
    def reconcile(sim_date: str, run_id=None) -> dict:
        return tasks.reconcile(sim_date, run_id)

    @task(retries=0)
    def quality_gate(sim_date: str, run_id=None) -> dict:
        return tasks.quality_gate(sim_date, run_id)

    @task
    def publish_report(sim_date: str, run_id=None) -> dict:
        return report.publish_report(sim_date, run_id)

    sim_date = resolve_sim_date()
    tariffs_loaded = load_tariffs(sim_date)
    wait_for_tariff_file(sim_date) >> tariffs_loaded

    compute = BashOperator(
        task_id="compute_daily_billing",
        bash_command=(
            "spark-submit --master 'local[2]' --driver-memory 768m "
            "--conf spark.ui.enabled=false "
            "/opt/grid/src/processing/batch_billing.py "
            "--sim-date '{{ ti.xcom_pull(task_ids=\"resolve_sim_date\") }}' "
            "--run-id '{{ run_id }}'"
        ),
        do_xcom_push=True,
    )

    reconciled = reconcile(sim_date)
    gate = quality_gate(sim_date)
    published = publish_report(sim_date)

    [tariffs_loaded, wait_for_late_readings(sim_date)] >> compute >> reconciled >> gate >> published


grid_daily_billing()
