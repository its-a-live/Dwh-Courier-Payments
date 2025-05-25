import logging
import pendulum
from airflow.decorators import dag, task
from stg.deliveries_dag.deliveries_loader import DeliveriesLoader  # Обновленный импорт для вашего модуля
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',  # Интервал выполнения DAG каждые 15 минут
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,  # Не запускать DAG для пропущенных дат
    tags=['stg', 'deliveries'],  # Теги для фильтрации в интерфейсе Airflow
    is_paused_upon_creation=True  # DAG будет в паузе при создании
)
def stg_deliveries_dag():
    # Создание подключения к базе данных для целевой системы
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    
    # Используем уже определенное подключение внутри DeliveriesLoader
    @task(task_id="deliveries_load")
    def load_deliveries():
        # Создаем экземпляр DeliveriesLoader, который сам управляет подключением к источнику
        deliveries_loader = DeliveriesLoader(pg_origin=dwh_pg_connect, pg_dest=dwh_pg_connect, log=log)
        deliveries_loader.load_deliveries()

    load_deliveries()

# Инициализация DAG
stg_deliveries_dag = stg_deliveries_dag()
