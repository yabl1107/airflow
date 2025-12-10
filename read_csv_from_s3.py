from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import csv

# -----------------------
# Función que reemplaza la Lambda
# -----------------------
def read_csv_from_s3(bucket: str, key: str):
    s3 = boto3.client('s3')

    try:
        # Obtener el archivo de S3
        response = s3.get_object(Bucket=bucket, Key=key)

        # Leer el contenido
        content = response['Body'].read().decode('utf-8').splitlines()
        reader = csv.reader(content)

        # Iterar las filas e imprimirlas en logs de Airflow
        for row in reader:
            print(row)

        print(f"Filas leídas: {len(content)}")
        return {"status": "ok", "rows_read": len(content)}

    except Exception as e:
        print(f"Error: {e}")
        return {"status": "error", "message": str(e)}

# -----------------------
# Configuración del DAG
# -----------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    's3_csv_reader_dag',
    default_args=default_args,
    description='Leer un CSV de S3 y mostrar filas',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2025, 11, 28),
    catchup=False,
    tags=['example'],
) as dag:

    # -----------------------
    # Tarea de lectura del CSV
    # -----------------------
    read_s3_csv = PythonOperator(
        task_id='read_s3_csv_task',
        python_callable=read_csv_from_s3,
        op_kwargs={
            'bucket': 'mi-bucket',     # Cambia por tu bucket
            'key': 'ruta/archivo.csv'  # Cambia por tu archivo
        }
    )

