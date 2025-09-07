# import all modules
import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)

# variable section
PROJECT_ID = "project-730830-am-ist"
REGION = "us-east1"
CLUSTER_NAME = "demo-cluster"
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

# define the dag
with DAG(
    dag_id="level_2_dag",
    schedule_interval="0 5 * * *",
    description="DAG to create a Dataproc cluster, run PySpark jobs, and delete the cluster",
    default_args = ARGS,
    tags=["pyspark","dataproc","etl", "data team"]
) as dag: 

# define the Tasks
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    pyspark_task_1 = DataprocSubmitJobOperator(
        task_id="pyspark_task_1", 
        job=PYSPARK_JOB_1, 
        region=REGION, 
        project_id=PROJECT_ID
    )
    pyspark_task_2 = DataprocSubmitJobOperator(
        task_id="pyspark_task_2", 
        job=PYSPARK_JOB_2, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
    )

# define the task sequence
create_cluster >> pyspark_task_1 >> pyspark_task_2 >> delete_cluster