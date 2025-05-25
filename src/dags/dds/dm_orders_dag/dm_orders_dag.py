import logging
import pendulum
from airflow.decorators import dag, task
from examples.dds.dm_orders_dag.orders_loader import OrdersLoader  # Импортируем загрузчик заказов
from lib import ConnectionBuilder

log = logging.getLogger(__name__)  # Создаем логгер для текущего модуля

@dag(
    schedule_interval='*/15 * * * *',  # Планируем выполнение DAG каждый день в 2:00 ночи.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,  # Отключаем подхват пропущенных запусков.
    tags=['dds', 'stg', 'dm_orders'],  # Добавляем теги для удобной классификации DAG.
    is_paused_upon_creation=False  # Устанавливаем состояние активным при создании.
)
def dm_orders_dag():  # Определяем функцию DAG
    pg_dest = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")  # Создаем подключение к базе данных

    @task(task_id="load_orders")  # Декорируем функцию как задачу в DAG
    def load_orders_task():  # Определяем функцию задачи
        loader = OrdersLoader(pg_dest, log)  # Создаем экземпляр загрузчика продуктов с подключением и логгером
        loader.load_orders()  # Вызываем метод для загрузки продуктов

    load_orders_task()  # Запускаем задачу загрузки продуктов

dm_orders_dag_instance = dm_orders_dag()  # Создаем экземпляр DAG
