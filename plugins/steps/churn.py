from sqlalchemy import MetaData, Table, Column, String, Integer, Float, DateTime, UniqueConstraint, inspect
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logger = logging.getLogger(__name__)

# Функция для создания таблицы
def create_table():
    hook = PostgresHook('destination_db')
    engine = hook.get_sqlalchemy_engine()
    
    metadata = MetaData()

    alt_users_churn_table = Table(
        'alt_users_churn',
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

    if not inspect(engine).has_table(alt_users_churn_table.name):
        logger.info("\u0421\u043e\u0437\u0434\u0430\u043d\u0438\u0435 \u0442\u0430\u0431\u043b\u0438\u0446\u044b alt_users_churn.")
        metadata.create_all(engine)
    else:
        logger.info("\u0422\u0430\u0431\u043b\u0438\u0446\u0430 alt_users_churn \u0443\u0436\u0435 \u0441\u0443\u0449\u0435\u0441\u0442\u0432\u0443\u0435\u0442.")

# Задача для извлечения дан\u043d\u044b\u0445
def extract(**kwargs):
    logger.info("\u0418\u0437\u0432\u043b\u0435\u0447\u0435\u043d\u0438\u0435 \u0434\u0430\u043d\u043d\u044b\u0445 \u0438\u0437 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0430.")
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
    logger.info("\u0414\u0430\u043d\u043d\u044b\u0435 \u0443\u0441\u043f\u0435\u0448\u043d\u043e \u0438\u0437\u0432\u043b\u0435\u0447\u0435\u043d\u044b.")
    return data

# Задача д\u043b\u044f \u043f\u0440\u0435\u043e\u0431\u0440\u0430\u0437\u043e\u0432\u0430\u043d\u0438\u044f \u0434\u0430\u043d\u043d\u044b\u0445
def transform(data: pd.DataFrame):
    logger.info("\u041f\u0440\u0435\u043e\u0431\u0440\u0430\u0437\u043e\u0432\u0430\u043d\u0438\u0435 \u0434\u0430\u043d\u043d\u044b\u0445.")
    data['target'] = (data['end_date'] != 'No').astype(int)
    data['end_date'].replace({'No': None}, inplace=True)
    logger.info("\u041f\u0440\u0435\u043e\u0431\u0440\u0430\u0437\u043e\u0432\u0430\u043d\u0438\u0435 \u0434\u0430\u043d\u043d\u044b\u0445 \u0437\u0430\u0432\u0435\u0440\u0448\u0435\u043d\u043e.")
    return data

# Зад\u0430\u0447\u0430 д\u043b\u044f \u0437\u0430\u0433\u0440\u0443\u0437\u043a\u0438 \u0434\u0430\u043d\u043d\u044b\u0445
def load(data: pd.DataFrame):
    logger.info("\u0417\u0430\u0433\u0440\u0443\u0437\u043a\u0430 \u0434\u0430\u043d\u043d\u044b\u0445 \u0432 \u0446\u0435\u043b\u0435\u0432\u0443\u044e \u0442\u0430\u0431\u043b\u0438\u0446\u0443.")
    hook = PostgresHook('destination_db')
    hook.insert_rows(
        table="alt_users_churn",
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=['customer_id'],
        rows=data.values.tolist()
    )
    logger.info("\u0414\u0430\u043d\u043d\u044b\u0435 \u0443\u0441\u043f\u0435\u0448\u043d\u043e \u0437\u0430\u0433\u0440\u0443\u0436\u0435\u043d\u044b.")
