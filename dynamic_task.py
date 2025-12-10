from airflow.decorators import dag, task
from datetime import datetime
import logging

@dag(
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['tutorial', 'dynamic_mapping']
)
def flujo_envio_emails():
    # 1. Tarea Productora (Genera la lista)
    @task
    def obtener_lista_emails():
        # Simulamos que esto viene de una Base de Datos
        return [
            "juan@empresa.com", 
            "maria@empresa.com", 
            "carlos@empresa.com", 
            "sofia@empresa.com"
        ]

    # 2. Tarea Consumidora (Sabe procesar SOLO UN email)
    @task
    def procesar_usuario(email: str):
        logging.info(f"--> Enviando notificación a: {email}")
        logging.info(f"OK: {email}")

    
    # Paso A: Obtener datos
    lista_emails = obtener_lista_emails()

    # Paso B: Mapeo Dinámico
    # Airflow crea 4 tareas en paralelo, una para cada dirección
    procesar_usuario.expand(email=lista_emails)

# Instanciar el DAG
dag_instancia = flujo_envio_emails()