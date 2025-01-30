import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DAG для подготовки данных
@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"]
)
def prepare_churn_dataset():

    # Функция для создания таблицы
    def create_table():
        from sqlalchemy import MetaData, Table, Column, String, Integer, Float, DateTime, UniqueConstraint, inspect

        hook = PostgresHook('destination_db')
        engine = hook.get_sqlalchemy_engine()
        
        metadata = MetaData()

        users_churn_table = Table(
            'users_churn',
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
            UniqueConstraint('customer_id', name='unique_customer_constraint')
        )

        if not inspect(engine).has_table(users_churn_table.name):
            logger.info("Создание таблицы users_churn.")
            metadata.create_all(engine)
        else:
            logger.info("Таблица users_churn уже существует.")

    # Задача для создания таблицы
    @task()
    def create_table_task():
        create_table()

    # Задача для извлечения данных
    @task()
    def extract(**kwargs):
        logger.info("Извлечение данных из источника.")
        hook = PostgresHook('source_db')
        conn = hook.get_conn()
        sql = f"""
        SELECT
            c.customer_id, c.begin_date, c.end_date, c.type, c.paperless_billing, c.payment_method,
            c.monthly_charges, c.total_charges, i.internet_service, i.online_security,
            i.online_backup, i.device_protection, i.tech_support, i.streaming_tv,
            i.streaming_movies, p.gender, p.senior_citizen, p.partner,
            p.dependents, ph.multiple_lines
        FROM contracts AS c
        LEFT JOIN internet AS i ON i.customer_id = c.customer_id
        LEFT JOIN personal AS p ON p.customer_id = c.customer_id
        LEFT JOIN phone AS ph ON ph.customer_id = c.customer_id
        """
        data = pd.read_sql(sql, conn)
        conn.close()
        logger.info("Данные успешно извлечены.")
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
            table="users_churn",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['customer_id'],
            rows=data.values.tolist()
        )
        logger.info("Данные успешно загружены.")

    # Вызов функции create_table внутри DAG
    create_table()  # Вызов функции для создания таблицы

    # Запуск задач в правильном порядке
    create_table_task()  # Создаем таблицу перед всеми другими задачами
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

# Инициализация DAG
prepare_churn_dataset()
