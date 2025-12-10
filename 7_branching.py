from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule # <--- Importante
from datetime import datetime
import random
# 1. Importas la clase Label
from airflow.utils.edgemodifier import Label

def generar_numero():
    return random.randint(1, 100)

def rama_decision(ti):
    numero = ti.xcom_pull(task_ids='generar')
    if numero % 2 == 0:
        return 'es_par'
    else:
        return 'es_impar'

with DAG(
    'branching_correcto', 
    start_date=datetime(2023, 1, 1), 
    schedule_interval='@once', 
    catchup=False
) as dag:


    dag.doc_md = """
        # DAG de Ventas Diarias
        Este DAG descarga las ventas de la API X y las carga en Redshift.
        **Owner:** Equipo de Data
        **SLA:** 1 hora
        """

    # 1. Generador
    t1 = PythonOperator(
        task_id='generar',
        python_callable=generar_numero
    )

    # Asignas el atributo a la variable de la tarea (t1)
    t1.doc_md = """
    ### Tarea Bash
    Esta tarea simplemente imprime un hola en la consola.
    *Importante:* No requiere conexión a DB.
    """

    # 2. Decisión (Branching)
    decision = BranchPythonOperator(
        task_id='decision_paridad',
        python_callable=rama_decision
    )

    # 3. Caminos
    par = BashOperator(
        task_id='es_par', 
        bash_command='echo "Es Par"'
    )

    impar = BashOperator(
        task_id='es_impar', 
        bash_command='echo "Es Impar"'
    )

    # 4. Tarea Final (EL RETO ESTÁ AQUÍ)
    despedida = BashOperator(
        task_id='despedida',
        bash_command='echo "Fin del proceso"',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS # ¿Qué regla va aquí?
    )

    # 5. Conexiones
    #t1 >> decision >> [par, impar] >> despedida

    # Conectamos el inicio
    t1 >> decision

    # AQUI ESTA LA MAGIA: Rompemos la lista para etiquetar cada flecha
    decision >> Label("El número es Par") >> par
    decision >> Label("El número es Impar") >> impar

    # Unimos todo al final
    [par, impar] >> despedida