from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'daniel',
    'start_date': datetime(2024, 3, 1),
    'retries': 1,
}

# 📅 Fecha actual en formato YYYYMMDD
today_str = datetime.now().strftime("%Y%m%d")

# Ruta base para logs customizados
log_dir = f"/Users/danielsanchezcuenca/Desktop/PROYECTO1/logs/log_{today_str}.log"

with DAG(
    dag_id='pipeline_tienda_v1',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    preprocesado = BashOperator(
        task_id='preprocesado_csv',
        bash_command=f'cd /Users/danielsanchezcuenca/Desktop/PROYECTO1 && source venv/bin/activate && python app/preprocesado.py'
    )

    insertar_bbdd = BashOperator(
        task_id='insertar_en_bbdd',
        bash_command=f'cd /Users/danielsanchezcuenca/Desktop/PROYECTO1 && source venv/bin/activate && python app/insert_bbdd.py'
    )

    modelo = BashOperator(
        task_id='entrenar_modelo',
        bash_command=f'cd /Users/danielsanchezcuenca/Desktop/PROYECTO1 && source venv/bin/activate && python app/modelo.py'
    )

    preprocesado >> insertar_bbdd >> modelo
