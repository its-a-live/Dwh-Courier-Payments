from pydantic import BaseModel
from typing import List
from psycopg import Connection
from psycopg.rows import class_row
from stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from logging import Logger
import requests
from datetime import datetime, timedelta

# Описание объекта Courier
class CourierObj(BaseModel):
    object_id: str
    object_value: str

# Репозиторий для загрузки данных из API
class CouriersOriginRepository:
    API_URL = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers'
    HEADERS = {
        'X-Nickname': 'user',
        'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f',
    }

    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_couriers(self, offset: int, limit: int, sort_field: str, sort_direction: str) -> List[CourierObj]:
        # Параметры пагинации, сортировки и ограничения количества записей
        params = {
            'limit': limit,
            'offset': offset,
            'sort_field': sort_field,
            'sort_direction': sort_direction
        }

        response = requests.get(self.API_URL, headers=self.HEADERS, params=params)
        data = response.json()

        couriers = [
            CourierObj(object_id=courier["_id"], object_value=str(courier))
            for index, courier in enumerate(data)
        ]
        return couriers

# Репозиторий для загрузки данных в целевую таблицу
class CouriersDestRepository:
    def insert_courier(self, conn: Connection, courier: CourierObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stg.couriers(object_id, object_value)
                VALUES (%(object_id)s, %(object_value)s)
                ON CONFLICT (object_id) DO UPDATE
                SET
                    object_value = EXCLUDED.object_value;
                """, {
                    "object_id": courier.object_id,
                    "object_value": courier.object_value
                }
            )

# Логика загрузки курьеров
class CouriersLoader:
    WF_KEY = "couriers_origin_to_stg_workflow"
    LAST_LOADED_OFFSET_KEY = "last_loaded_offset"
    BATCH_LIMIT = 150  # Размер пакета загрузки

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CouriersOriginRepository(pg_origin)
        self.stg = CouriersDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_couriers(self):
        # Дата 7 дней назад
        seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                # Если нет записей в таблице настроек, создаем новую
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_OFFSET_KEY: 0})

            # Последний загруженный offset
            last_loaded_offset = wf_setting.workflow_settings[self.LAST_LOADED_OFFSET_KEY]
            
            # Параметры сортировки
            sort_field = "id"  # например, сортировка по id
            sort_direction = "asc"  # сортировка по возрастанию

            offset = last_loaded_offset
            while True:
                # Загружаем порцию данных, начиная с текущего offset
                load_queue = self.origin.list_couriers(offset, self.BATCH_LIMIT, sort_field, sort_direction)
                self.log.info(f"Found {len(load_queue)} couriers to load.")
                
                if not load_queue:
                    self.log.info("No more data to load.")
                    break

                for courier in load_queue:
                    self.stg.insert_courier(conn, courier)

                # Обновляем offset для следующей итерации
                offset += self.BATCH_LIMIT
                wf_setting.workflow_settings[self.LAST_LOADED_OFFSET_KEY] = offset
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished up to offset {offset}.")
