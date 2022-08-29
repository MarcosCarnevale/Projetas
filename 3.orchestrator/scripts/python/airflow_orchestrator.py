from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

#===============================================================================
# Create parameter date to run the dag
yesterday = datetime.today() - timedelta(days=1)
# Format: YYYYM
year = yesterday.strftime("%Y")
month = str(int(yesterday.strftime("%m")))
date_parameter_vra = year + month

#--------------------------------------------------------------------------------
# Create parameter date to run the dag
yesterday = datetime.today() - timedelta(days=1)
# Format: YYYYMM
date_parameter_year_month = yesterday.strftime("%Y%m")

#===============================================================================
# Define the DAG
dag = DAG(
    dag_id="orchestrator",
    start_date=datetime(2021, 1, 1),
    # Schedule the DAG to run once a day at 00:30
    schedule_interval="30 0 * * *",
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    default_args={
        "owner": "airflow",
        "depends_on_past": False
    }
)

#===============================================================================
path_bronze = "/home/0.bronze/scripts/shell/"

#===============================================================================
# Define the tasks for the DAG to run scripts in specific directories
task_bronze_vra = BashOperator(
    task_id="bronze_vra",
    bash_command=f"sh {path_bronze}executor_bronze_vra.sh",
    # Pass the parameter date to the script
    env={"date_parameter": date_parameter_vra},
    dag=dag
)

task_bronze_air_cia = BashOperator(
    tasks_id="bronze_air_cia",
    bash_command=f"sh {path_bronze}executor_bronze_air_cia.sh",
    # Pass the parameter date to the script
    env={"date_parameter": date_parameter_year_month},
    dag=dag
)

task_bronze_aerodromo = BashOperator(
    tasks_id="bronze_aerodromo",
    bash_command=f"sh {path_bronze}executor_bronze_aerodromo.sh",
    dag=dag
)

# Define execution asynchronism
task_bronze_vra >> task_bronze_air_cia >> task_bronze_aerodromo
#===============================================================================
path_silver = "/home/1.silver/scripts/shell/"

#===============================================================================
# Define the tasks for the DAG to run scripts in specific directories
task_silver_vra = BashOperator(
    task_id="silver_vra",
    bash_command=f"sh {path_silver}executor_silver_vra.sh",
    # Pass the parameter date to the script
    env={"date_parameter": date_parameter_year_month},
    dag=dag
)

task_silver_air_cia = BashOperator(
    tasks_id="silver_air_cia",
    bash_command=f"sh {path_silver}executor_silver_air_cia.sh",
    # Pass the parameter date to the script
    env={"date_parameter": date_parameter_year_month},
    dag=dag
)

task_silver_aerodromo = BashOperator(
    tasks_id="silver_aerodromo",
    bash_command=f"sh {path_silver}executor_silver_aerodromo.sh",
    dag=dag
)

# Define execution asynchronism
task_silver_vra >> task_silver_air_cia >> task_silver_aerodromo

#===============================================================================








