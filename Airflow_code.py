# import all modules
import airflow
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models import Variable
from airflow.utils.email import send_email


# callback for failures
def notify_email(context):
    alert_email = Variable.get("alert_email")
    subject = f"Airflow Task Failed: {context['task_instance'].task_id}"
    body = f"""
    DAG: {context['dag'].dag_id}
    Task: {context['task_instance'].task_id}
    Execution Time: {context['execution_date']}
    Log URL: {context['task_instance'].log_url}
    """
    send_email(to=alert_email, subject=subject, html_content=body)


# dummy function for PythonOperator (if you still need it later)
def check_csv_files_today():
    print("Checking for today’s CSV files...")
    return True


# variable section
PROJECT_ID = "project-730830-am-ist"
REGION = "us-east1"
CLUSTER_NAME = "demo-cluster"
BQ_DATASET = "demo_dataset"
BQ_TABLE = "processed_data"

ARGS = {
    "owner": "shaik saidhul",
    "email_on_failure": True,
    "email_on_retry": True,
    "email": "****@gmail.com",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "start_date": days_ago(1),
}

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "e2-standard-2",
        "disk_config": {"boot_disk_type": "pd-balanced", "boot_disk_size_gb": 32},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "e2-standard-2",
        "disk_config": {"boot_disk_type": "pd-balanced", "boot_disk_size_gb": 32},
    },
}

PYSPARK_JOB_1 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": "gs://src-test-bkt-sdk/dummy_pyspark_job_1.py"},
}

PYSPARK_JOB_2 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": "gs://src-test-bkt-sdk/dummy_pyspark_job_2.py"},
}


# define the DAG
with DAG(
    dag_id="level_2_dag",
    schedule_interval="0 5 * * *",   # every day at 5am UTC
    description="DAG to create a Dataproc cluster, run PySpark jobs, load to BigQuery, and delete the cluster",
    default_args=ARGS,
    tags=["pyspark", "dataproc", "etl", "bigquery", "data team"],
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        on_failure_callback=notify_email,
    )

    wait_for_file = GCSObjectExistenceSensor(
        task_id="wait_for_file_in_gcs",
        bucket="src-test-bkt-sdk",  # update with real bucket
        object="path/to/your/file.csv",  # update with real object path
        timeout=300,
        poke_interval=30,
        mode="poke",
        on_failure_callback=notify_email,
    )

    pyspark_task_1 = DataprocSubmitJobOperator(
        task_id="pyspark_task_1",
        job=PYSPARK_JOB_1,
        region=REGION,
        project_id=PROJECT_ID,
        on_failure_callback=notify_email,
    )

    pyspark_task_2 = DataprocSubmitJobOperator(
        task_id="pyspark_task_2",
        job=PYSPARK_JOB_2,
        region=REGION,
        project_id=PROJECT_ID,
        on_failure_callback=notify_email,
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id="load_to_bq",
        bucket="src-test-bkt-sdk",
        source_objects=["output/*.csv"],  # GCS path from PySpark job
        destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}",
        schema_update_options=["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"],
        write_disposition="WRITE_TRUNCATE",
        skip_leading_rows=1,
        source_format="CSV",
        field_delimiter=",",
        autodetect=True,
        on_failure_callback=notify_email,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        on_failure_callback=notify_email,
    )

    # define the task sequence
    create_cluster >> wait_for_file >> pyspark_task_1 >> pyspark_task_2 >> load_to_bq >> delete_cluster
