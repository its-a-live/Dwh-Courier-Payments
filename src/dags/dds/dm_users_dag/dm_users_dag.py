import logging
import pendulum
from airflow.decorators import dag, task
from examples.dds.dm_users_dag.users_loader import UsersLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)  # Создаем логгер для текущего модуля

@dag(
    schedule_interval='0/15 * * * *',  # Планируем выполнение DAG каждый день в 1:00 ночи.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,  # Отключаем подхват пропущенных запусков.
    tags=['dds', 'stg', 'dm_users'],  # Добавляем теги для удобной классификации DAG.
    is_paused_upon_creation=False  # Устанавливаем состояние активным при создании.
)
def dm_users_dag():  # Определяем функцию DAG
    pg_dest = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")  # Создаем подключение к базе данных

    @task(task_id="load_users")  # Декорируем функцию как задачу в DAG
    def load_users_task():  # Определяем функцию задачи
        loader = UsersLoader(pg_dest, log)  # Создаем экземпляр загрузчика пользователей с подключением и логгером
        loader.load_users()  # Вызываем метод для загрузки пользователей

    load_users_task()  # Запускаем задачу загрузки пользователей

dm_users_dag_instance = dm_users_dag()  # Создаем экземпляр DAG
