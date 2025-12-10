from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import random
import logging

# 1. EL EMISOR (Simplemente retorna el valor)
def generar_numero():
    numero = random.randint(1, 100)
    logging.info(f"Generé el número: {numero}")
    return numero  # <--- ¡Esto crea el XCom automáticamente

# 2. EL RECEPTOR (Recibe 'ti' para poder hacer pull)
def verificar_paridad(ti):
    # Jalar el dato. 
    numero_recibido = ti.xcom_pull(task_ids='generar_numerito')
    # Si no especificamos key usará key='return_value' es el nombre por defecto cuando usas 'return'
    # numero_recibido = ti.xcom_pull(task_ids='generar_numerito', key='return_value')
    
    if not numero_recibido:
        raise ValueError("¡No recibí nada!")

    logging.info(f"Recibí el {numero_recibido} desde XCom.")

    if numero_recibido % 2 == 0:
        ti.xcom_push(key='message', value="el valor !es par!. Mssge verified")
    else:
        ti.xcom_push(key='message', value="el valor !es impar!. Mssge verified")

def share_message(ti):
    #Podemos obtener valores de task anteriores.
    numero = ti.xcom_pull(task_ids='generar_numerito',key='return_value')
    message_recieved = ti.xcom_pull(task_ids='verificar_paridad', key='message')

    logging.info(f""" I've got a message : 
    {message_recieved.upper()}
    Also. The created number was {numero}.
    Thanks.
    )""")
    

with DAG(
    'xcoms_overview', 
    start_date=datetime(2023, 1, 1), 
    schedule_interval=None, 
    catchup=False
    ) as dag:

    t1 = PythonOperator(
        task_id='generar_numerito',
        python_callable=generar_numero
    )

    t2 = PythonOperator(
        task_id='verificar_paridad',
        python_callable=verificar_paridad
    )

    t3=PythonOperator(
        task_id='compartir_mensaje',
        python_callable=share_message
    )

    t1 >> t2 >> t3


