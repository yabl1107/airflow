import random
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from datetime import datetime, timedelta


def generar_numero():
    # TU CÓDIGO AQUÍ:
    # 1. Genera un número (ej: random.randint(1, 100))
    ranNum = random.randint(1,100)

    try:
        # 2. Abre el archivo en /opt/airflow/data/numero_suerte.txt y escribe el número
        with open('/opt/airflow/data/numero_suerte.txt','w') as file:
            # OJO: write() solo acepta texto, así que convertimos el int a str
            file.write(str(ranNum))
        pass
    except Exception as e:
        logging.error(f"❌ ¡Alerta! Falló la función 'generar_numero'. Causa: {e}")
        raise e

with DAG(
    'mi_primer_python_op', 
    start_date=datetime(2023, 1, 1), 
    schedule_interval='@once',
    catchup=False
    ) as dag:
    
    tarea_generar = PythonOperator(
        task_id='generar_datos',
        python_callable=generar_numero
    )