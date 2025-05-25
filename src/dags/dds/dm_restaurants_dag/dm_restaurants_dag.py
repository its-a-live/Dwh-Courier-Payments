import logging
import pendulum
from airflow.decorators import dag, task
from examples.dds.dm_restaurants_dag.restaurants_loader import RestaurantsLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)  # Создаем логгер для текущего модуля

@dag(
    schedule_interval='0/15 * * * *',  # Планируем выполнение DAG каждый день в 1:00 ночи.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,  # Отключаем подхват пропущенных запусков.
    tags=['dds', 'stg', 'dm_restaurants'],  # Добавляем теги для удобной классификации DAG.
    is_paused_upon_creation=False  # Устанавливаем состояние активным при создании.
)
def dm_restaurants_dag():  # Определяем функцию DAG
    pg_dest = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")  # Создаем подключение к базе данных

    @task(task_id="load_restaurants")  # Декорируем функцию как задачу в DAG
    def load_restaurants_task():  # Определяем функцию задачи
        loader = RestaurantsLoader(pg_dest, log)  # Создаем экземпляр загрузчика пользователей с подключением и логгером
        loader.load_restaurants()  # Вызываем метод для загрузки пользователей

    load_restaurants_task()  # Запускаем задачу загрузки пользователей

dm_restaurants_dag_instance = dm_restaurants_dag()  # Создаем экземпляр DAG
