import logging
import pendulum
from airflow.decorators import dag, task
from examples.dds.dm_products_dag.products_loader import ProductsLoader  # Импортируем загрузчик продуктов
from lib import ConnectionBuilder

log = logging.getLogger(__name__)  # Создаем логгер для текущего модуля

@dag(
    schedule_interval='0/15 * * * *',  # Планируем выполнение DAG каждый день в 2:00 ночи.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,  # Отключаем подхват пропущенных запусков.
    tags=['dds', 'stg', 'dm_products'],  # Добавляем теги для удобной классификации DAG.
    is_paused_upon_creation=False  # Устанавливаем состояние активным при создании.
)
def dm_products_dag():  # Определяем функцию DAG
    pg_dest = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")  # Создаем подключение к базе данных

    @task(task_id="load_products")  # Декорируем функцию как задачу в DAG
    def load_products_task():  # Определяем функцию задачи
        loader = ProductsLoader(pg_dest, log)  # Создаем экземпляр загрузчика продуктов с подключением и логгером
        loader.load_products()  # Вызываем метод для загрузки продуктов

    load_products_task()  # Запускаем задачу загрузки продуктов

dm_products_dag_instance = dm_products_dag()  # Создаем экземпляр DAG
