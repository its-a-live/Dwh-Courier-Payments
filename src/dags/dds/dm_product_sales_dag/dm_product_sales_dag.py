import logging
import pendulum
from airflow.decorators import dag, task
from examples.dds.dm_product_sales_dag.product_sales_loader import ProductSalesLoader  # Импортируем загрузчик заказов
from lib import ConnectionBuilder

log = logging.getLogger(__name__)  # Создаем логгер для текущего модуля

@dag(
    schedule_interval='0/15 * * * *',  # Планируем выполнение DAG каждый день в 2:00 ночи.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,  # Отключаем подхват пропущенных запусков.
    tags=['dds', 'stg', 'dm_orders'],  # Добавляем теги для удобной классификации DAG.
    is_paused_upon_creation=False  # Устанавливаем состояние активным при создании.
)
def dm_product_sales_dag():  # Определяем функцию DAG
    pg_dest = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")  # Создаем подключение к базе данных

    @task(task_id="load_product_sales")  # Декорируем функцию как задачу в DAG
    def load_product_sales_task():  # Определяем функцию задачи
        loader = ProductSalesLoader(pg_dest, log)  # Создаем экземпляр загрузчика продуктов с подключением и логгером
        loader.load_product_sales()  # Вызываем метод для загрузки продуктов

    load_product_sales_task()  # Запускаем задачу загрузки продуктов

dm_product_sales_dag_instance = dm_product_sales_dag()  # Создаем экземпляр DAG
