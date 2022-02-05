#Importing supporting libraries
import os
from datetime import datetime, timedelta

#Importing core airflow libraries
from airflow import DAG
from airflow.models.dag import dag
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.mysql_hook import MySqlHook

#Importing airflow emr operators
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator

# AWS Configurations
BUCKET_NAME = "personal0project"
s3_data = "raw"
s3_script = "script/processing_online_retail_data.py"
s3_clean = "stage/"


#Define EMR cluster type as a dict
JOB_FLOW_OVERRIDES = {
    "Name": "Batch Project EMR 220205002",
    "ReleaseLabel": "emr-5.29.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                }
            ],
        }
    ],
    "Instances": {
        "Ec2KeyName" : "Ec2KEY_PersonalProject", #Use your own key here
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT",
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}
SPARK_STEPS = [ # Note the params values are supplied to the operator
    {
        "Name": "Move raw data from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://{{ params.BUCKET_NAME }}/{{ params.s3_data }}",
                "--dest=/movie",
            ],
        },
    },
    {
        "Name": "process online retail data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script }}",
            ],
        },
    },
    {
        "Name": "Move clean data from HDFS to S3",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=/output",
                "--dest=s3://{{ params.BUCKET_NAME }}/{{ params.s3_clean }}",
            ],
        },
    },
]




    
#Creating all the necessary functions for the python operator
def _mysql_to_local(sql_str:str,file_name:str):
    mysql = MySqlHook(mysql_conn_id='mysql_default')
    result_df = mysql.get_pandas_df(sql_str) #Memory Limit step
    result_df.to_csv(file_name)

def _local_to_s3(bucket_name:str,key:str,file_name:str,remove_local:bool=False):
    s3 = S3Hook()
    s3.load_file(filename = file_name,key =key,bucket_name = bucket_name,replace = True)
    if remove_local == True:
        if os.path.isfile(file_name) == True:
            os.remove(file_name)

#Create default args for each task
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="beginner_batch_processing"
    ,default_args=default_args
    ,description="Batch processing project using mysql ,S3 ,EMR and Redshift"
    ,schedule_interval=timedelta(days=1)
    ,start_date=datetime(2022,2,5)
    ,catchup=False
) as dag :
    
    start_of_data_pipeline = DummyOperator(task_id="start_of_data_pipeline")
    
    extract_online_retail_data = PythonOperator( 
        task_id="extract_online_retail_data"
        ,python_callable=_mysql_to_local
        ,op_kwargs={
            "sql_str": "SELECT * FROM project.movieonlineretail ;"
            ,"file_name": "OnlineRetail.csv"
        }
    )

    load_raw_data_to_s3 = PythonOperator(
        task_id = "load_raw_data_to_s3"
        ,python_callable=_local_to_s3
        ,op_kwargs={
            "file_name": "OnlineRetail.csv"
            ,"key": "raw/online_retail.csv"
            ,"bucket_name": "personal0project"
            ,"remove_local": "false"
        }
    )
    
    upload_spark_script_to_s3 = PythonOperator(
        task_id = "upload_spark_script_to_s3"
        ,python_callable=_local_to_s3
        ,op_kwargs={
            "file_name": "/opt/airflow/data/scripts/processing_online_retail_data.py" #Path inside of the container
            ,"key": "script/processing_online_retail_data.py"
            ,"bucket_name": "personal0project"
            ,"remove_local": "false"
        }
    )
    
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id = "create_emr_cluster"
        ,job_flow_overrides = JOB_FLOW_OVERRIDES
        #,aws_conn_id = "aws_default"
        #,emr_conn_id = "emr_default" #This is most likely no longer needed 
        
    )
    
    
    add_emr_steps = EmrAddStepsOperator(
        task_id = "add_emr_steps"
        ,job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}"
        ,aws_conn_id="aws_default"
        ,steps=SPARK_STEPS
        ,params={
            "BUCKET_NAME": BUCKET_NAME
            ,"s3_data": s3_data
            ,"s3_script": s3_script
            ,"s3_clean": s3_clean
        }
    )
    
    last_step = len(SPARK_STEPS) - 1 #last step of the spark script

    # wait for the steps to complete
    check_emr_step = EmrStepSensor(
        task_id="check_emr_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_emr_steps', key='return_value')["
        + str(last_step)
        + "] }}",
        aws_conn_id="aws_default"
    )
        # Terminate the EMR cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default"
    )
    
    end_of_data_pipeline = DummyOperator(task_id="end_of_data_pipeline")
    
    
    #Define Pipeline tasks dependency 
    start_of_data_pipeline >> extract_online_retail_data >> [load_raw_data_to_s3,upload_spark_script_to_s3] >> create_emr_cluster
    create_emr_cluster >> add_emr_steps >> check_emr_step >> terminate_emr_cluster >> end_of_data_pipeline
    


