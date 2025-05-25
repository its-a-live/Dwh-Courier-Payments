from logging import Logger
from datetime import datetime
from lib.dict_util import str2json
from lib import PgConnect
from typing import Any, List, Dict
from psycopg import Connection
import json

class OrdersDestRepository:
    def insert_order(self, conn: Connection, orders_data: dict, user_id: str, restaurant_id: str, timestamp_id: str, delivery_id: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.dm_orders (order_key, user_id, restaurant_id, order_status, timestamp_id, delivery_id)
                VALUES (%(order_key)s, %(user_id)s, %(restaurant_id)s, %(order_status)s, %(timestamp_id)s, %(delivery_id)s)
                ON CONFLICT (order_key) 
                DO UPDATE SET
                    order_status = EXCLUDED.order_status,
                    timestamp_id = EXCLUDED.timestamp_id,
                    delivery_id = EXCLUDED.delivery_id;
                """,
                {
                    "order_key": orders_data["_id"],
                    "user_id": user_id,
                    "restaurant_id": restaurant_id,
                    "order_status": orders_data["final_status"],
                    "timestamp_id": timestamp_id,
                    "delivery_id": delivery_id
                }
            )

class OrdersLoader:
    WF_KEY = "dm_orders_load_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 12000

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.dest = OrdersDestRepository()
        self.log = log

    def load_orders(self) -> None:
        with self.pg_dest.client() as conn:
            wf_setting = self.get_last_loaded_ts(conn)
            last_loaded_ts = wf_setting[self.LAST_LOADED_TS_KEY]

            orders_to_load = self.get_orders_from_stg(last_loaded_ts)
            for order_data in orders_to_load:
                user_id = self.get_user_id(conn, order_data["user"]["id"])  # Получаем id пользователя
                restaurant_id = self.get_restaurant_id(conn, order_data["restaurant"]["id"])  # Получаем id ресторана
                timestamp_id = self.get_timestamp_id(conn, order_data["update_ts"])  # Получаем id временной метки
                delivery_id = self.get_delivery_id(conn, order_data["_id"])  # Получаем id доставки

                if timestamp_id is None or delivery_id is None:
                    continue  # Пропускаем запись, если временная метка или доставка не найдены.

                # Вставляем заказ в базу данных
                self.dest.insert_order(conn, order_data, user_id, restaurant_id, timestamp_id, delivery_id)

            new_last_loaded_ts = max(order["update_ts"] for order in orders_to_load)
            self.save_last_loaded_ts(conn, new_last_loaded_ts)

            self.log.info(f"Loaded {len(orders_to_load)} orders.")

    def get_orders_from_stg(self, last_loaded_ts: datetime) -> List[Dict[str, Any]]:
        with self.pg_dest.client() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT object_value
                FROM stg.ordersystem_orders
                WHERE update_ts > %(last_loaded_ts)s
                ORDER BY update_ts ASC
                LIMIT %(limit)s;
                """,
                {
                    "last_loaded_ts": last_loaded_ts,
                    "limit": self.BATCH_LIMIT
                }
            )
            objs = cur.fetchall()
            return [str2json(obj[0]) for obj in objs]

    def get_user_id(self, conn: Connection, user_id: str) -> str:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id 
                FROM dds.dm_users
                WHERE user_id = %s;
                """,
                (user_id,)
            )
            result = cur.fetchone()
            if result:
                return result[0]
            raise ValueError(f"User with id {user_id} not found.")

    def get_restaurant_id(self, conn: Connection, restaurant_id: str) -> str:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id 
                FROM dds.dm_restaurants
                WHERE restaurant_id = %s;
                """,
                (restaurant_id,)
            )
            result = cur.fetchone()
            if result:
                return result[0]
            raise ValueError(f"Restaurant with id {restaurant_id} not found.")

    def get_timestamp_id(self, conn: Connection, timestamp_id: str) -> str:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id 
                FROM dds.dm_timestamps
                WHERE ts = %s;
                """,
                (timestamp_id,)
            )
            result = cur.fetchone()
            if result:
                return result[0]
            self.log.warning(f"Timestamp with id {timestamp_id} not found. Skipping this order.")
            return None  # Возвращаем None, если временная метка не найдена.

    def get_delivery_id(self, conn: Connection, order_id: str) -> str:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id 
                FROM dds.dm_deliveries
                WHERE delivery_id = (
                    SELECT (replace(object_value::text, '''', '"')::jsonb) ->> 'delivery_id'
                    FROM stg.deliveries
                    WHERE (replace(object_value::text, '''', '"')::jsonb) ->> 'order_id' = %s
                );
                """,
                (order_id,)
            )
            result = cur.fetchone()
            if result:
                return result[0]
            self.log.warning(f"Delivery for order_id {order_id} not found. Skipping this order.")
            return None  # Возвращаем None, если доставка не найдена.

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
