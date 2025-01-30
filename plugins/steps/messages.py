from airflow.providers.telegram.hooks.telegram import TelegramHook  # Импортируем хук Telegram

# Функция для отправки сообщения об успешном выполнении DAG
def send_telegram_success_message(context):
    # Используем ваши данные для подключения
    hook = TelegramHook(telegram_conn_id='test', token='7867245353:AAHpPEtRmpBwNb9B0oB871uEFlV_XAwHZi0', chat_id='-4794909845')
    dag = context['dag'].dag_id
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!'  # Определение текста сообщения
    hook.send_message({
        'chat_id': '-4794909845',  # Используем ваш chat_id
        'text': message
    })  # Отправление сообщения

# Функция для отправки сообщения о неудачном выполнении DAG
def send_telegram_failure_message(context):
    # Используем ваши данные для подключения
    hook = TelegramHook(telegram_conn_id='test', token='7867245353:AAHpPEtRmpBwNb9B0oB871uEFlV_XAwHZi0', chat_id='-4794909845')
    dag = context['task_instance_key_str']
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло неуспешно!'  # Определение текста сообщения
    hook.send_message({
        'chat_id': '-4794909845',  # Используем ваш chat_id
        'text': message
    })  # Отправление сообщения
