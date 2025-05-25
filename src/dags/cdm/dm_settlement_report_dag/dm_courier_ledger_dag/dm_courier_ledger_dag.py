import logging
import pendulum
from airflow.decorators import dag, task
from examples.cdm.dm_settlement_report_dag.dm_courier_ledger_dag.courier_ledger_loader import CourierLedgerLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)  

@dag(
    schedule_interval='*/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    tags=['dds', 'stg', 'dm_orders'],
    is_paused_upon_creation=False
)
def dm_courier_ledger_dag():
    pg_dest = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="load_courier_ledger")
    def load_courier_ledger_task():
        loader = CourierLedgerLoader(pg_dest) 
        loader.load_courier_ledger() 

    load_courier_ledger_task()

dm_courier_ledger_dag_instance = dm_courier_ledger_dag()
