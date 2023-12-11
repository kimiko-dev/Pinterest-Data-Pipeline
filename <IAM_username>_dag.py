from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta 


#Define params for Submit Run Operator
notebook_task = {
    'notebook_path': '<path_to_databricks_notebook>',
}

default_args = {
    'owner': '<desired_name>',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

#Creating the DAG.
with DAG('<IAM_username>_dag',
    start_date=datetime.now(),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:


    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id='databricks_default',
        existing_cluster_id='<cluster_id>',
        notebook_task=notebook_task
    )
    opr_submit_run