from logging import Logger
from datetime import datetime
from lib.dict_util import str2json
from lib import PgConnect
from typing import Any, List, Dict
from psycopg import Connection
import json
class ProductSalesDestRepository:
    def insert_product_sales(self, conn: Connection, product_sales_data: dict, product_id: str, order_id: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.fct_product_sales (product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                VALUES (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                ON CONFLICT (product_id, order_id) do nothing;
                """,
                {
                    "product_id": product_id,
                    "order_id": order_id,
                    "count": product_sales_data["quantity"],
                    "price": product_sales_data["price"],
                    "total_sum": product_sales_data["product_cost"],
                    "bonus_payment": product_sales_data["bonus_payment"],
                    "bonus_grant": product_sales_data["bonus_grant"]
                }
            )
class ProductSalesLoader:
    WF_KEY = "dm_product_sales_load_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 10000000
    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.dest = ProductSalesDestRepository()
        self.log = log
    def load_product_sales(self) -> None:
        with self.pg_dest.client() as conn:
            wf_setting = self.get_last_loaded_ts(conn)
            last_loaded_ts = wf_setting[self.LAST_LOADED_TS_KEY]
            product_sales_to_load = self.get_product_sales_from_stg(last_loaded_ts)
            
            for product_sales_data in product_sales_to_load:
                # Получаем id заказа
                try:
                    order_id = self.get_order_id(conn, product_sales_data["order_id"])
                except ValueError as e:
                    self.log.warning(f"Order with id {product_sales_data['order_id']} not found, skipping...")
                    continue  # Пропускаем, если order_id не найден
                
                for product_data in product_sales_data["product_payments"]:
                    product_id = self.get_product_id(conn, product_data["product_id"])  # Получаем id продукута
                    # Вставляем заказ в базу данных
                    self.dest.insert_product_sales(conn, product_data, product_id, order_id)
            new_last_loaded_ts = max(order["order_date"] for order in product_sales_to_load)
            self.save_last_loaded_ts(conn, new_last_loaded_ts)
            self.log.info(f"Loaded {len(product_sales_to_load)} product sales.")
    def get_product_sales_from_stg(self, last_loaded_ts: datetime) -> List[Dict[str, Any]]:
        with self.pg_dest.client() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT event_value
                FROM stg.bonussystem_events
                WHERE event_ts > %(last_loaded_ts)s
                and event_type = 'bonus_transaction'
                ORDER BY event_ts ASC
                LIMIT %(limit)s;
                """,
                {
                    "last_loaded_ts": last_loaded_ts,
                    "limit": self.BATCH_LIMIT
                }
            )
            objs = cur.fetchall()
            return [str2json(obj[0]) for obj in objs]
    def get_product_id(self, conn: Connection, product_id: str) -> str:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id 
                FROM dds.dm_products
                WHERE product_id = %s;
                """,
                (product_id,)
            )
            result = cur.fetchone()
            if result:
                return result[0]
            raise ValueError(f"User with id {product_id} not found.")
    def get_order_id(self, conn: Connection, order_id: str) -> str:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id 
                FROM dds.dm_orders
                WHERE order_key = %s;
                """,
                (order_id,)
            )
            result = cur.fetchone()
            if result:
                return result[0]
            raise ValueError(f"Order with id {order_id} not found.")
    def get_last_loaded_ts(self, conn: Connection) -> dict:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT workflow_settings
                FROM dds.srv_wf_settings
                WHERE workflow_key = %(wf_key)s;
                """, {"wf_key": self.WF_KEY}
            )
            wf_setting = cur.fetchone()
            if wf_setting and len(wf_setting) > 0:
                return wf_setting[0]
            return {self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()}
    def save_last_loaded_ts(self, conn: Connection, last_loaded_ts: datetime) -> None:
        if isinstance(last_loaded_ts, str):
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                VALUES (%(wf_key)s, %(wf_setting)s)
                ON CONFLICT (workflow_key) DO UPDATE
                SET workflow_settings = EXCLUDED.workflow_settings;
                """,
                {
                    "wf_key": self.WF_KEY,
                    "wf_setting": json.dumps({"last_loaded_ts": last_loaded_ts.isoformat()})
                }
            )
            