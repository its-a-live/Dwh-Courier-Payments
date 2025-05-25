import logging
import pendulum
from airflow.decorators import dag, task
from examples.dds.dm_deliveries_dag.deliveries_loader import DeliveriesLoader  # Импортируем загрузчик доставок
from lib import ConnectionBuilder

log = logging.getLogger(__name__)  # Создаем логгер для текущего модуля

@dag(
    schedule_interval='*/15 * * * *',  # Планируем выполнение DAG каждые 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,  # Отключаем подхват пропущенных запусков.
    tags=['dds', 'stg', 'dm_deliveries'],  # Добавляем теги для классификации DAG.
    is_paused_upon_creation=False  # Устанавливаем DAG активным при создании.
)
def dm_deliveries_dag():  # Определяем функцию DAG
    pg_dest = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")  # Создаем подключение к базе данных

    @task(task_id="load_deliveries")  # Декорируем функцию как задачу в DAG
    def load_deliveries_task():  # Определяем функцию задачи
        loader = DeliveriesLoader(pg_dest, log)  # Создаем экземпляр загрузчика доставок с подключением и логгером
        loader.load_deliveries()  # Вызываем метод для загрузки доставок

    load_deliveries_task()  # Запускаем задачу загрузки доставок

dm_deliveries_dag_instance = dm_deliveries_dag()  # Создаем экземпляр DAG
