import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.sensors.python import PythonSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformWorkflowInvocationOperator,
)
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator

# Fetch Airflow Variables dynamically
PREFIX = Variable.get("dev_qb_prefix", default_var="dev_qb")
GCS_BUCKET = Variable.get("dev_qb_gcs_bucket")
GCP_PROJECT = Variable.get("dev_qb_project_id")
DATAFORM_REGION = Variable.get("dev_qb_dataform_region", default_var="asia-south1")

default_args = {
    'owner': PREFIX,
    'depends_on_past': True,
    'start_date': datetime(2026, 10, 10),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id=f"{PREFIX}_quickbite",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    catchup=True,
    max_active_runs=1,
) as dag:

    # 1. Wait for daily files in GCS
    def check_files_exist(ds, **kwargs):
        gcs = GCSHook()
        files = ['restaurants', 'customers', 'orders', 'order_items']
        for f in files:
            path = f"capstone/{PREFIX}/landing/{ds}/{f}_{ds}.csv"
            if not gcs.exists(bucket_name=GCS_BUCKET, object_name=path):
                return False
        return True

    wait_for_daily_files = PythonSensor(
        task_id="wait_for_daily_files",
        python_callable=check_files_exist,
        poke_interval=120,
        timeout=1800,
        mode="reschedule"
    )

    # 2. Load Raw CSVs to BigQuery Raw
    load_tasks = []
    for entity in ['restaurants', 'customers', 'orders', 'order_items']:
        load_raw = GCSToBigQueryOperator(
            task_id=f"load_raw_{entity}",
            bucket=GCS_BUCKET,
            source_objects=[f"capstone/{PREFIX}/landing/{{{{ ds }}}}/{entity}_{{{{ ds }}.csv"],
            destination_project_dataset_table=f"{GCP_PROJECT}.{PREFIX}_raw.{entity}",
            write_disposition="WRITE_TRUNCATE",
            skip_leading_rows=1,
            source_format="CSV",
            autodetect=True,
        )
        load_tasks.append(load_raw)

    # 3. Dataform Compilation & Workflow Execution
    compile_dataform = DataformCreateCompilationResultOperator(
        task_id="compile_dataform",
        project_id=GCP_PROJECT,
        region=DATAFORM_REGION,
        repository_id=f"{PREFIX}-quickbite",
        compilation_result={"git_commitish": "main"},
    )

    run_dataform = DataformWorkflowInvocationOperator(
        task_id="run_dataform",
        project_id=GCP_PROJECT,
        region=DATAFORM_REGION,
        repository_id=f"{PREFIX}-quickbite",
        compilation_result_id=f"{{{{ task_instance.xcom_pull(task_ids='compile_dataform')['name'] }}}}",
    )

    # 4. Move Processed Landing Files to Archive
    def archive_landing_files(ds, **kwargs):
        gcs = GCSHook()
        files = ['restaurants', 'customers', 'orders', 'order_items']
        for f in files:
            src = f"capstone/{PREFIX}/landing/{ds}/{f}_{ds}.csv"
            dest = f"capstone/{PREFIX}/archive/{ds}/{f}_{ds}.csv"
            if gcs.exists(bucket_name=GCS_BUCKET, object_name=src):
                gcs.copy(source_bucket=GCS_BUCKET, source_object=src, destination_object=dest)
                gcs.delete(bucket_name=GCS_BUCKET, object_name=src)

    archive_files = PythonOperator(
        task_id="archive_landing_files",
        python_callable=archive_landing_files
    )

    # Workflow Dependency Topology
    wait_for_daily_files >> load_tasks >> compile_dataform >> run_dataform >> archive_files