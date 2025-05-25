import logging
import pendulum
from airflow.decorators import dag, task
from examples.dds.dm_couriers_dag.couriers_loader import CouriersLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)  # Создаем логгер для текущего модуля

@dag(
    schedule_interval='0/15 * * * *',  # Планируем выполнение DAG каждый день в 1:00 ночи.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,  # Отключаем подхват пропущенных запусков.
    tags=['dds', 'stg', 'dm_couriers'],  # Добавляем теги для удобной классификации DAG.
    is_paused_upon_creation=False  # Устанавливаем состояние активным при создании.
)
def dm_couriers_dag():  # Определяем функцию DAG
    pg_dest = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")  # Создаем подключение к базе данных

    @task(task_id="load_couriers")  # Декорируем функцию как задачу в DAG
    def load_couriers_task():  # Определяем функцию задачи
        loader = CouriersLoader(pg_dest, log)  # Создаем экземпляр загрузчика пользователей с подключением и логгером
        loader.load_couriers()  # Вызываем метод для загрузки пользователей

    load_couriers_task()  # Запускаем задачу загрузки пользователей

dm_couriers_dag_instance = dm_couriers_dag()  # Создаем экземпляр DAG
