import logging
import pendulum
from airflow.decorators import dag, task
from stg.couriers_dag.couriers_loader import CouriersLoader  # Обновленный импорт для вашего модуля
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',  # Интервал выполнения DAG каждые 15 минут
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,  # Не запускать DAG для пропущенных дат
    tags=['stg', 'couriers'],  # Теги для фильтрации в интерфейсе Airflow
    is_paused_upon_creation=True  # DAG будет в паузе при создании
)
def stg_couriers_dag():
    # Создание подключения к базе данных для целевой системы
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    
    # Используем уже определенное подключение внутри CouriersLoader
    @task(task_id="couriers_load")
    def load_couriers():
        # Создаем экземпляр CouriersLoader, который сам управляет подключением к источнику
        couriers_loader = CouriersLoader(pg_origin=dwh_pg_connect, pg_dest=dwh_pg_connect, log=log)
        couriers_loader.load_couriers()

    load_couriers()

# Инициализация DAG
stg_couriers_dag = stg_couriers_dag()
