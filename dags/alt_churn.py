import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from steps.churn import create_table, extract, transform, load  # Импорт функций из плагина
import logging
from steps.messages import send_telegram_success_message, send_telegram_failure_message

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация DAG
with DAG(
    dag_id='churn',
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"],
    on_success_callback=send_telegram_success_message,  # Успешный вызов
    on_failure_callback=send_telegram_failure_message   # Неудачный вызов
) as dag:

    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table
    )

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
        op_kwargs={'data': '{{ task_instance.xcom_pull(task_ids="extract") }}'}
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
        op_kwargs={'data': '{{ task_instance.xcom_pull(task_ids="transform") }}'}
    )

    # Определение порядка выполнения задач
    create_table_task >> extract_task >> transform_task >> load_task
