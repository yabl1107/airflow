from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import logging
import requests
from datetime import datetime, timedelta
import os


def download_image(url, ruta_destino):
    try:
        # Crear el directorio si no existe
        carpeta = os.path.dirname(ruta_destino)
        os.makedirs(carpeta, exist_ok=True)

        response = requests.get(url)
        response.raise_for_status()

        with open(ruta_destino, "wb") as f:
            f.write(response.content)

        logging.info(f"Imagen guardada en: {ruta_destino}")

    except Exception as e:
        logging.error(f"Error en download_image: {e}")
        raise



with DAG(
    dag_id="testVariables",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    tarea_descarga_imagen = PythonOperator(
        task_id="descargar_imagen",
        python_callable=download_image,
        op_kwargs={
            "url":"https://store.fcbarcelona.com/cdn/shop/files/4x5_balde_01.jpg?v=1763802982&width=1946",
            "ruta_destino":f"{Variable.get('mi_ruta_base')}/imagenBarca.png"
        }
    )


    crear_archivo = BashOperator(
        task_id="crear_archivo",
        do_xcom_push=False,
        bash_command="""
            pwd;
            mkdir -p {{ var.value.mi_ruta_base }}/carpetaBarcelona ;
            echo "Hola, soy creado por un bash. Esta carpeta es barcelonense" > {{ var.value.mi_ruta_base }}/carpetaBarcelona/archivoVariable.txt
            mv {{ var.value.mi_ruta_base }}/*.png {{ var.value.mi_ruta_base }}/carpetaBarcelona
        """
    )

    tarea_descarga_imagen >> crear_archivo
