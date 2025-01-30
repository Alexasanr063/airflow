import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"]
)
def clean_churn_dataset():

    # Функция для создания таблицы
    @task()
    def create_table():
        from sqlalchemy import MetaData, Table, Column, String, Integer, Float, DateTime, UniqueConstraint, inspect

        hook = PostgresHook('destination_db')
        engine = hook.get_sqlalchemy_engine()
        
        metadata = MetaData()

        churn_table = Table(
            'clean_users_churn',
            metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('customer_id', String, nullable=False),
            Column('begin_date', DateTime),
            Column('end_date', DateTime),
            Column('type', String),
            Column('paperless_billing', String),
            Column('payment_method', String),
            Column('monthly_charges', Float),
            Column('total_charges', Float),
            Column('internet_service', String),
            Column('online_security', String),
            Column('online_backup', String),
            Column('device_protection', String),
            Column('tech_support', String),
            Column('streaming_tv', String),
            Column('streaming_movies', String),
            Column('gender', String),
            Column('senior_citizen', Integer),
            Column('partner', String),
            Column('dependents', String),
            Column('multiple_lines', String),
            Column('target', Integer),
            UniqueConstraint('customer_id', name='unique_clean_customer_constraint')
        )

        if not inspect(engine).has_table(churn_table.name):
            logger.info("Создание таблицы clean_users_churn.")
            metadata.create_all(engine)
        else:
            logger.info("Таблица clean_users_churn уже существует.")

    # Задача для извлечения данных
    @task()
    def extract():
        logger.info("Извлечение данных из таблицы users_churn.")
        hook = PostgresHook('source_db')
        conn = hook.get_conn()
        sql = """
        SELECT
            customer_id, begin_date, end_date, type, paperless_billing, payment_method,
            monthly_charges, total_charges, internet_service, online_security,
            online_backup, device_protection, tech_support, streaming_tv,
            streaming_movies, gender, senior_citizen, partner,
            dependents, multiple_lines
        FROM users_churn
        """
        data = pd.read_sql(sql, conn)
        conn.close()
        logger.info(f"Данные успешно извлечены. Количество строк: {data.shape[0]}")
        return data

    # Задача для преобразования данных
    @task()
    def transform(data: pd.DataFrame):
        logger.info("Преобразование данных.")
        data['target'] = (data['end_date'] != 'No').astype(int)
        data['end_date'].replace({'No': None}, inplace=True)
        logger.info("Преобразование данных завершено.")
        return data

    # Задача для загрузки данных
    @task()
    def load(data: pd.DataFrame):
        logger.info("Загрузка данных в целевую таблицу.")
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="clean_users_churn",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['customer_id'],
            rows=data.values.tolist()
        )
        logger.info("Данные успешно загружены.")

    # Запуск задач в правильном порядке
    create_table()  # Создаем таблицу перед всеми другими задачами
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

# Инициализация DAG
clean_churn_dataset()

