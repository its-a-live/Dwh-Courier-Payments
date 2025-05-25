import json
from datetime import date, datetime, time
from typing import Optional
from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from examples.dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting
from examples.dds.order_repositories_test import OrderJsonObj, OrderRawRepository

# Класс для временных меток заказов
class TimestampDdsObj(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    time: time
    date: date

# Класс для временных меток доставки
class DeliveryTimestampDdsObj(BaseModel):
    id: int
    delivery_ts: datetime
    year: int
    month: int
    day: int
    time: time
    date: date

class TimestampDdsRepository:
    def insert_dds_timestamp(self, conn: Connection, timestamp: TimestampDdsObj) -> None:
        with conn.cursor() as cur:
            print(f"Inserting order timestamp: {timestamp}")
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, time, date)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s)
                    ON CONFLICT (ts) DO UPDATE
                    SET ts = EXCLUDED.ts, 
                        year = EXCLUDED.year, 
                        month = EXCLUDED.month, 
                        day = EXCLUDED.day, 
                        time = EXCLUDED.time, 
                        date = EXCLUDED.date;
                """,
                {
                    "ts": timestamp.ts,
                    "year": timestamp.year,
                    "month": timestamp.month,
                    "day": timestamp.day,
                    "time": timestamp.time,
                    "date": timestamp.date
                },
            )

    def insert_delivery_timestamp(self, conn: Connection, timestamp: DeliveryTimestampDdsObj) -> None:
        with conn.cursor() as cur:
            print(f"Inserting delivery timestamp: {timestamp}")
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, time, date)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s)
                    ON CONFLICT (ts) DO UPDATE
                    SET ts = EXCLUDED.ts, 
                        year = EXCLUDED.year, 
                        month = EXCLUDED.month, 
                        day = EXCLUDED.day, 
                        time = EXCLUDED.time, 
                        date = EXCLUDED.date;
                """,
                {
                    "ts": timestamp.delivery_ts,
                    "year": timestamp.delivery_ts.year,
                    "month": timestamp.delivery_ts.month,
                    "day": timestamp.delivery_ts.day,
                    "time": timestamp.delivery_ts.time(),
                    "date": timestamp.delivery_ts.date()
                },
            )

    def get_timestamp(self, conn: Connection, dt: datetime) -> Optional[TimestampDdsObj]:
        with conn.cursor(row_factory=class_row(TimestampDdsObj)) as cur:
            print(f"Fetching timestamp for: {dt}")
            cur.execute(
                """
                    SELECT id, ts, year, month, day, time, date
                    FROM dds.dm_timestamps
                    WHERE ts = %(dt)s;
                """,
                {"dt": dt},
            )
            obj = cur.fetchone()
        return obj

class TimestampLoader:
    ORDER_WF_KEY = "order_timestamp_workflow"
    DELIVERY_WF_KEY = "delivery_timestamp_workflow"
    LAST_LOADED_ORDER_ID_KEY = "last_loaded_order_id"
    LAST_LOADED_DELIVERY_ID_KEY = "last_loaded_delivery_id"

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.dwh = pg
        self.raw_orders = OrderRawRepository()
        self.dds = TimestampDdsRepository()
        self.settings_repository = settings_repository

    def parse_order_ts(self, order_raw: OrderJsonObj) -> TimestampDdsObj:
        order_json = json.loads(order_raw.object_value)
        print(f"Parsing order timestamp from: {order_json['date']}")
        dt = datetime.strptime(order_json['date'], "%Y-%m-%d %H:%M:%S")
        t = TimestampDdsObj(id=0,
                            ts=dt,
                            year=dt.year,
                            month=dt.month,
                            day=dt.day,
                            time=dt.time(),
                            date=dt.date()
                            )
        return t

    def parse_delivery_ts(self, delivery_raw: dict) -> DeliveryTimestampDdsObj:
        delivery_ts = delivery_raw['delivery_ts'].strip()
        print(f"Parsing delivery timestamp from: {delivery_ts}")
        try:
            dt = datetime.strptime(delivery_ts, "%Y-%m-%d %H:%M:%S.%f")
            print(f"Parsed successfully with milliseconds: {dt}")
        except ValueError:
            dt = datetime.strptime(delivery_ts, "%Y-%m-%d %H:%M:%S")
            print(f"Parsed successfully without milliseconds: {dt}")
        
        t = DeliveryTimestampDdsObj(id=0,
                                     delivery_ts=dt,
                                     year=dt.year,
                                     month=dt.month,
                                     day=dt.day,
                                     time=dt.time(),
                                     date=dt.date()
                                     )
        return t

    def load_timestamps(self):
        with self.dwh.connection() as conn:
            print("Loading order timestamps...")
            # Загрузка таймстампов заказов
            order_wf_setting = self.settings_repository.get_setting(conn, self.ORDER_WF_KEY)
            if not order_wf_setting:
                order_wf_setting = EtlSetting(id=0, workflow_key=self.ORDER_WF_KEY, workflow_settings={self.LAST_LOADED_ORDER_ID_KEY: -1})
            last_loaded_order_id = order_wf_setting.workflow_settings[self.LAST_LOADED_ORDER_ID_KEY]
            load_queue = self.raw_orders.load_raw_orders(conn, last_loaded_order_id)
            print(f"Orders to load: {len(load_queue)}")
            for order in load_queue:
                ts_to_load = self.parse_order_ts(order)
                self.dds.insert_dds_timestamp(conn, ts_to_load)
                order_wf_setting.workflow_settings[self.LAST_LOADED_ORDER_ID_KEY] = order.id
                self.settings_repository.save_setting(conn, order_wf_setting)

            print("Loading delivery timestamps...")
            # Загрузка временных меток доставки
            delivery_wf_setting = self.settings_repository.get_setting(conn, self.DELIVERY_WF_KEY)
            if not delivery_wf_setting:
                delivery_wf_setting = EtlSetting(id=0, workflow_key=self.DELIVERY_WF_KEY, workflow_settings={self.LAST_LOADED_DELIVERY_ID_KEY: -1})
            last_loaded_delivery_id = delivery_wf_setting.workflow_settings[self.LAST_LOADED_DELIVERY_ID_KEY]

            delivery_load_query = """
                SELECT id, object_id, object_value
                FROM stg.deliveries
                WHERE id > %(last_loaded_delivery_id)s
                ORDER BY id ASC;
            """
            with conn.cursor() as cur:
                cur.execute(delivery_load_query, {"last_loaded_delivery_id": last_loaded_delivery_id})
                delivery_load_queue = cur.fetchall()

            print(f"Deliveries to load: {len(delivery_load_queue)}")
            for delivery in delivery_load_queue:
                delivery_id = delivery[0]           # delivery[0] — это id
                delivery_object_value = delivery[2]  # delivery[2] — это object_value

                # Заменяем одинарные на двойные кавычки и загружаем JSON
                print(f"Processing delivery ID: {delivery_id}")
                delivery_json = json.loads(delivery_object_value.replace("'", '"'))
                ts_to_load_delivery = self.parse_delivery_ts(delivery_json)
                self.dds.insert_delivery_timestamp(conn, ts_to_load_delivery)
                delivery_wf_setting.workflow_settings[self.LAST_LOADED_DELIVERY_ID_KEY] = delivery_id
                self.settings_repository.save_setting(conn, delivery_wf_setting)
