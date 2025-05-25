from logging import Logger
from lib import PgConnect
from typing import Any, List, Dict
from psycopg import Connection
import json

class CouriersDestRepository:
    def insert_courier(self, conn: Connection, courier_data: dict) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.dm_couriers (courier_id, courier_name)
                VALUES (%(id)s, %(name)s)
                ON CONFLICT (courier_id) DO UPDATE
                SET 
                    courier_name = EXCLUDED.courier_name;
                """,
                {
                    "id": courier_data["_id"],
                    "name": courier_data["name"],
                }
            )

class CouriersLoader:
    WF_KEY = "dm_couriers_load_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.dest = CouriersDestRepository()
        self.log = log

    def load_couriers(self) -> None:
        with self.pg_dest.client() as conn:
            wf_setting = self.get_last_loaded_id(conn)
            last_loaded_id = wf_setting[self.LAST_LOADED_ID_KEY]

            couriers_to_load = self.get_couriers_from_stg(last_loaded_id)
            if not couriers_to_load:
                self.log.info("No new couriers found.")
                return

            for courier_data in couriers_to_load:
                self.dest.insert_courier(conn, courier_data)  # Вставляем каждого курьера в базу данных

            # Сохраняем прогресс
            new_last_loaded_id = max(courier_data["_id"] for courier_data in couriers_to_load)  # Получаем максимальный ID
            self.save_last_loaded_id(conn, new_last_loaded_id)  # Сохраняем последнее загруженное значение id

            self.log.info(f"Loaded {len(couriers_to_load)} couriers.")  # Логируем количество загруженных курьеров

    def safe_str2json(self, s: str) -> dict:
        # Безопасное преобразование строки в JSON с обработкой ошибок
        try:
            return json.loads(s.replace("'", '"'))  # Заменяем одинарные кавычки на двойные
        except json.JSONDecodeError as e:
            self.log.error(f"Ошибка декодирования JSON: {e}")
            return {}  # Возвращаем пустой словарь в случае ошибки

    def get_couriers_from_stg(self, last_loaded_id: str) -> List[Dict[str, Any]]:
        # Метод для получения курьеров из STG, обновленных после last_loaded_id
        with self.pg_dest.client() as conn:  # Используем клиент для соединения с БД
            cur = conn.cursor()  # Получаем курсор от соединения
            cur.execute(
                """
                SELECT object_value
                FROM stg.couriers
                WHERE object_id > %(last_loaded_id)s  -- Загружаем только те записи, где ID больше последнего загруженного
                ORDER BY object_id ASC  -- Сортируем по возрастанию ID
                LIMIT %(limit)s;
                """,
                {
                    "last_loaded_id": last_loaded_id,  # Используем последний загруженный ID как строку
                    "limit": self.BATCH_LIMIT  # Лимит на количество загружаемых записей
                }
            )
            objs = cur.fetchall()  # Получаем все результаты запроса
            return [self.safe_str2json(obj[0]) for obj in objs]  # Преобразуем результаты в JSON с защитой от ошибок

    def get_last_loaded_id(self, conn: Connection) -> dict:
        # Метод для получения последнего загруженного id из базы данных
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT workflow_settings
                FROM dds.srv_wf_settings
                WHERE workflow_key = %(wf_key)s;
                """, {"wf_key": self.WF_KEY}  # Используем ключ рабочего процесса
            )
            wf_setting = cur.fetchone()  # Возвращает первую строку результата запроса как кортеж.
            if wf_setting and len(wf_setting) > 0:  # Проверяем, что wf_setting не None и имеет элементы.
                return wf_setting[0]  # Возвращаем первый элемент кортежа, который уже является словарем.
            return {self.LAST_LOADED_ID_KEY: "0"}  # Возвращаем строковое значение по умолчанию, если данных нет.

    def save_last_loaded_id(self, conn: Connection, last_loaded_id: str) -> None:
        # Метод для сохранения последнего загруженного id в базе данных
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                VALUES (%(wf_key)s, %(wf_setting)s)
                ON CONFLICT (workflow_key) DO UPDATE
                SET workflow_settings = EXCLUDED.workflow_settings;
                """,
                {
                    "wf_key": self.WF_KEY,  # Используем ключ рабочего процесса
                    "wf_setting": json.dumps({"last_loaded_id": last_loaded_id})  # Сериализация в JSON
                }
            )
