from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="crear_archivo_con_bash",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    crear_archivo = BashOperator(
        task_id="crear_archivo",
        # Desactiva el push automático para no ensuciar la DB
        do_xcom_push=False,
        bash_command="""
            pwd;
            mkdir -p /opt/airflow/carpetaSiLocasa ;
            echo "Hola, soy creado por un bash" > /opt/airflow/carpetaSiLocasa/archivo.txt
        """
    )

    crear_archivo
