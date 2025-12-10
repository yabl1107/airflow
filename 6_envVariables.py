## DAG que utiliza variables de entorno

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import random
import logging
import os
from airflow.models import Variable


def calcularImpuesto(precioBase = 100):
    # Variable de entorno añadida en docker compose file AIRFLOW_VAR_TASA_IMPUESTO: "0.18"
    #AIRFLOW_VAR_TASA_IMPUESTO : airflow ignora el prefijo  AIRFLOW_VAR_, solo lo usa para saber que es una variable entorno
    #Existe prioridad en las variables que lee, 1ero busca en env y luego en definidas en UI
    tasa_impuesto = Variable.get("TASA_IMPUESTO")
    logging.info(f"El tipo de dato de variable es {type(tasa_impuesto)}")
    tasa_impuesto = float(tasa_impuesto)
    logging.info(f"La tasa de impuesto obtenida con Variable.get es {tasa_impuesto}")
    logging.info(f"El valor del impuesto para precio {precioBase} es : {precioBase*tasa_impuesto}")


with DAG(
    'envTest_DAG',
    schedule_interval = None,
    start_date = datetime(2025,1,1),
    catchup = False
) as dag:

    calcularImpuesto = PythonOperator(
        task_id = 'calcular_impuesto_task',
        python_callable = calcularImpuesto,
        op_kwargs={
            "precioBase": 200        }
    )

    calcularImpuesto