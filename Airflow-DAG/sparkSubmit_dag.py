import os
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import (
   DataprocCreateClusterOperator,
   DataprocDeleteClusterOperator,
   DataprocSubmitJobOperator
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.google.cloud.hooks.secrets_manager import SecretsManagerHook

DAG_ID = "dataproc_nyc_food_inspection_dwh"
PROJECT_ID = "nyc-food-data-modeling"
CLUSTER_NAME =  "nyc-food-inspection-dwh-airflow-cluster"
REGION = "us-east1"
STORAGE_BUCKET = "nyc_food_inspection_dwh"
PIP_INSTALL_PATH = f"gs://goog-dataproc-initialization-actions-{REGION}/python/pip-install.sh"

SECRET_MANAGER_HOOK = SecretsManagerHook(project_id=PROJECT_ID)
SECRET_ID = "composer_key"
SERVICE_ACCOUNT_KEY = SECRET_MANAGER_HOOK.get_secret_value(secret_id=SECRET_ID)


DIMENSION_JOB_FILE_URI = "gs://nyc_food_inspection_dwh/Code/NYC_Food_Dimensions.py"
FACT_FOOD_INSPECTION_JOB_FILE_URI = "gs://nyc_food_inspection_dwh/Code/NYC_FACT_Food_Inspection.py"
FACT_FOOD_INSPECTION_VIOLATION_JOB_FILE_URI = "gs://nyc_food_inspection_dwh/Code/NYC_FACT_Food_Inspection_violation.py"

YESTERDAY = datetime.now() - timedelta(days=1)

default_dag_args = {
    'depends_on_past': False,
    'start_date': YESTERDAY,
}

# Cluster definition

# CLUSTER_CONFIG = {
#    "master_config": {
#        "num_instances": 1,
#        "machine_type_uri": "n2-standard-2",
#        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
#    },
#    "worker_config": {
#        "num_instances": 2,
#        "machine_type_uri": "n2-standard-2",
#        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
#    },
# }

# Cluster Config for Dataproc Cluster

CLUSTER_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    storage_bucket=STORAGE_BUCKET,

    #Master Node Details
    num_masters=1,
    master_machine_type="n2-standard-2",
    master_disk_type="pd-standard",
    master_disk_size=50,

    #Worker Node Details
    num_workers=2,
    worker_machine_type="n1-standard-2",
    worker_disk_type="pd-standard",
    worker_disk_size=50,

    properties={},
    image_version="2.1-ubuntu20",
    autoscaling_policy=None,
    idle_delete_ttl=1800,
    metadata={"PIP_PACKAGES": 'apache-airflow apache-airflow-providers-google google-api-python-client google-auth-oauthlib google-auth-httplib2 sodapy'},
    init_actions_uris=[
                    PIP_INSTALL_PATH
                ],
).make()

CLUSTER_CONFIG["service_account_secrets"] = {
    "service_account": SERVICE_ACCOUNT_KEY
}


# Pyspark Job submit definition
DIMENSION_PYSPARK_JOB = {
   "reference": {"project_id": PROJECT_ID},
   "placement": {"cluster_name": CLUSTER_NAME},
   "pyspark_job": {"main_python_file_uri": DIMENSION_JOB_FILE_URI},
   }

FACT_FOOD_INSPECTION_PYSPARK_JOB = {
   "reference": {"project_id": PROJECT_ID},
   "placement": {"cluster_name": CLUSTER_NAME},
   "pyspark_job": {"main_python_file_uri": FACT_FOOD_INSPECTION_JOB_FILE_URI},
   }

FACT_FOOD_INSPECTION_VIOLATION_PYSPARK_JOB = {
   "reference": {"project_id": PROJECT_ID},
   "placement": {"cluster_name": CLUSTER_NAME},
   "pyspark_job": {"main_python_file_uri": FACT_FOOD_INSPECTION_VIOLATION_JOB_FILE_URI},
   }

# DAG to create the Cluster
with DAG(
    DAG_ID,
    schedule="@once",
    default_args=default_dag_args,
    description='A simple DAG to create a Dataproc workflow',
   ) as dag:

   # [START how_to_cloud_dataproc_create_cluster_operator]
   create_cluster = DataprocCreateClusterOperator(
       task_id="create_cluster",
       project_id=PROJECT_ID,
       cluster_config=CLUSTER_CONFIG,
       region=REGION,
       cluster_name=CLUSTER_NAME,

   )

   # [Submit a job to the cluster]
   dimension_pyspark_task = DataprocSubmitJobOperator(
       task_id="dimension_pyspark_task", 
       job=DIMENSION_PYSPARK_JOB, 
       region=REGION, 
       project_id=PROJECT_ID
   )

   # [Submit a job to the cluster]
   fact_food_inspection_pyspark_task = DataprocSubmitJobOperator(
       task_id="fact_food_inspection_pyspark_task", 
       job=FACT_FOOD_INSPECTION_PYSPARK_JOB, 
       region=REGION, 
       project_id=PROJECT_ID
   )

   # [Submit a job to the cluster]
   fact_food_inspection_violation_pyspark_task = DataprocSubmitJobOperator(
       task_id="fact_food_inspection_violation_pyspark_task", 
       job=FACT_FOOD_INSPECTION_VIOLATION_PYSPARK_JOB, 
       region=REGION, 
       project_id=PROJECT_ID
   )


   # [Delete the cluster after Job Ends]
   delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )



   create_cluster >>  dimension_pyspark_task >> fact_food_inspection_pyspark_task >> fact_food_inspection_violation_pyspark_task >> delete_cluster