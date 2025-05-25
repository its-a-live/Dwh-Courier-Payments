import logging
import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from examples.stg.bonus_system_events_dag.events_loader import EventLoader

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['stg', 'origin', 'events'],
    is_paused_upon_creation=False  # Запуск дага сразу после создания
)
def stg_bonus_system_events_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Таск для загрузки событий из outbox
    @task(task_id="events_load")
    def load_events():
        event_loader = EventLoader(origin_pg_connect, dwh_pg_connect, log)
        event_loader.load_events()

    load_events()

stg_bonus_system_events_dag = stg_bonus_system_events_dag()
