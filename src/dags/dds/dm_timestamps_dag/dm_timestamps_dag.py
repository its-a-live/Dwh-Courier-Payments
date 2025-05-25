import logging
import pendulum
from airflow.decorators import dag, task
from examples.dds.dm_timestamps_dag.timestamp_loader import TimestampLoader
from examples.dds.dds_settings_repository import DdsEtlSettingsRepository
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['dds', 'stg', 'dm_timestamps'],
    is_paused_upon_creation=False
)
def dm_timestamps_dag():
    pg_dest = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="load_order_timestamps")
    def load_order_timestamps_task():
        settings_repository = DdsEtlSettingsRepository()
        loader = TimestampLoader(pg_dest, settings_repository)
        loader.load_timestamps()  # Загрузка таймстампов о заказах

    @task(task_id="load_delivery_timestamps")
    def load_delivery_timestamps_task():
        settings_repository = DdsEtlSettingsRepository()
        loader = TimestampLoader(pg_dest, settings_repository)
        loader.load_timestamps()  # Загрузка таймстампов о доставках

    load_order_timestamps_task()  # Запуск задачи загрузки таймстампов о заказах
    load_delivery_timestamps_task()  # Запуск задачи загрузки таймстампов о доставках

dm_timestamps_dag_instance = dm_timestamps_dag()
