from logging import Logger
from datetime import datetime
from lib.dict_util import str2json
from lib import PgConnect
from typing import Any, List, Dict
from psycopg import Connection
import json

class DeliveriesDestRepository:
    def insert_delivery(self, conn: Connection, delivery_data: dict, courier_id: str, timestamp_id: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.dm_deliveries (delivery_id, courier_id, address, rate, tip_sum, timestamp_id)
                VALUES (%(delivery_id)s, %(courier_id)s, %(address)s, %(rate)s, %(tip_sum)s, %(timestamp_id)s)
                ON CONFLICT (delivery_id) 
                DO UPDATE SET
                    courier_id = EXCLUDED.courier_id,
                    address = EXCLUDED.address,
                    rate = EXCLUDED.rate,
                    tip_sum = EXCLUDED.tip_sum,
                    timestamp_id = EXCLUDED.timestamp_id;
                """,
                {
                    "delivery_id": delivery_data["delivery_id"],
                    "courier_id": courier_id,
                    "address": delivery_data["address"],
                    "rate": delivery_data["rate"],
                    "tip_sum": delivery_data["tip_sum"],
                    "timestamp_id": timestamp_id
                }
            )

class DeliveriesLoader:
    WF_KEY = "dm_deliveries_load_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.dest = DeliveriesDestRepository()
        self.log = log

    def load_deliveries(self) -> None:
        with self.pg_dest.client() as conn:
            wf_setting = self.get_last_loaded_id(conn)
            last_loaded_id = wf_setting[self.LAST_LOADED_ID_KEY]

            deliveries_to_load = self.get_deliveries_from_stg(last_loaded_id)
            if not deliveries_to_load:
                self.log.info("No new deliveries found.")
                return

            for delivery_data in deliveries_to_load:
                courier_id = self.get_courier_id(conn, delivery_data["courier_id"])  # Получаем id курьера
                timestamp_id = self.get_timestamp_id(conn, delivery_data["delivery_ts"])  # Получаем id временной метки

                if not (courier_id and timestamp_id):
                    self.log.warning("Missing foreign key references. Skipping delivery.")
                    continue

                self.dest.insert_delivery(conn, delivery_data, courier_id, timestamp_id)

            new_last_loaded_id = max(delivery_data["delivery_id"] for delivery_data in deliveries_to_load)
            self.save_last_loaded_id(conn, new_last_loaded_id)

            self.log.info(f"Loaded {len(deliveries_to_load)} deliveries.")

    def safe_str2json(self, s: str) -> dict:
        try:
            return json.loads(s.replace("'", '"'))
        except json.JSONDecodeError as e:
            self.log.error(f"JSON decoding error: {e}")
            return {}

    def get_deliveries_from_stg(self, last_loaded_id: str) -> List[Dict[str, Any]]:
        with self.pg_dest.client() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT object_id, object_value
                FROM stg.deliveries
                WHERE object_id > %(last_loaded_id)s
                ORDER BY object_id ASC
                LIMIT %(limit)s;
                """,
                {
                    "last_loaded_id": last_loaded_id,
                    "limit": self.BATCH_LIMIT
                }
            )
            objs = cur.fetchall()
            return [self.safe_str2json(obj[1]) for obj in objs]

    def get_courier_id(self, conn: Connection, courier_id: str) -> str:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id 
                FROM dds.dm_couriers
                WHERE courier_id = %s;
                """,
                (courier_id,)
            )
            result = cur.fetchone()
            if result:
                return result[0]
            self.log.warning(f"Courier with id {courier_id} not found.")
            return None

    def get_timestamp_id(self, conn: Connection, timestamp: str) -> str:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id 
                FROM dds.dm_timestamps
                WHERE ts = %s;
                """,
                (timestamp,)
            )
            result = cur.fetchone()
            if result:
                return result[0]
            self.log.warning(f"Timestamp with ts {timestamp} not found.")
            return None

    def get_last_loaded_id(self, conn: Connection) -> dict:
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
            return {self.LAST_LOADED_ID_KEY: "0"}

    def save_last_loaded_id(self, conn: Connection, last_loaded_id: str) -> None:
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
                    "wf_setting": json.dumps({"last_loaded_id": last_loaded_id})
                }
            )
