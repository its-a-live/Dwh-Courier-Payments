import logging
import pendulum
from airflow.decorators import dag, task
from examples.cdm.dm_settlement_report_dag.settlement_report_loader import SettlementReportLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)  

@dag(
    schedule_interval='*/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['dds', 'stg', 'dm_orders'],
    is_paused_upon_creation=False
)
def dm_settlement_report_dag():
    pg_dest = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="load_settlement_report")
    def load_settlement_report_task():
        loader = SettlementReportLoader(pg_dest) 
        loader.load_report_by_days() 

    load_settlement_report_task()

dm_settlement_report_dag_instance = dm_settlement_report_dag()
