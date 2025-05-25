from logging import Logger
from datetime import datetime
from lib.dict_util import str2json
from lib import PgConnect
from typing import Any, List, Dict 
from psycopg import Connection 
import json

class RestaurantsDestRepository:
    def insert_restaurant(self, conn: Connection, restaurant_data: dict) -> None:
        with conn.cursor() as cur:
            # Сначала закрываем предыдущие записи, если имя ресторана изменилось
            cur.execute(
                """
                UPDATE dds.dm_restaurants 
                SET active_to = %(update_ts)s
                WHERE restaurant_id = %(id)s
                AND active_to = '2099-12-31 00:00:00'
                AND restaurant_name != %(name)s;
                """,
                {
                    "id": restaurant_data["_id"],  # id ресторана
                    "name": restaurant_data["name"],  # новое имя ресторана
                    "update_ts": restaurant_data["update_ts"]  # метка времени
                }
            )

            # Затем вставляем новую запись с актуальной информацией
            cur.execute(
                """
                INSERT INTO dds.dm_restaurants (restaurant_id, restaurant_name, active_from, active_to)
                VALUES (%(id)s, %(name)s, %(update_ts)s, '2099-12-31 00:00:00')
                ON CONFLICT (restaurant_id) DO NOTHING;
                """,
                {
                    "id": restaurant_data["_id"],  # id ресторана
                    "name": restaurant_data["name"],  # имя ресторана
                    "update_ts": restaurant_data["update_ts"]  # метка времени
                }
            )


class RestaurantsLoader:
    WF_KEY = "dm_restaurants_load_workflow"  # Ключ для рабочего процесса загрузки ресторанов
    LAST_LOADED_TS_KEY = "last_loaded_ts"  # Ключ для хранения времени последней загрузки
    BATCH_LIMIT = 100  # Лимит на количество загружаемых пользователей за раз

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        # Инициализация класса с соединением к PostgreSQL и логгером
        self.pg_dest = pg_dest
        self.dest = RestaurantsDestRepository()  # Создаем экземпляр репозитория пользователей
        self.log = log  # Устанавливаем логгер

    def load_restaurants(self) -> None:
        # Метод для загрузки пользователей из источника данных
        with self.pg_dest.client() as conn:  # Используем клиент для соединения с БД
            wf_setting = self.get_last_loaded_ts(conn)  # Получаем последние настройки рабочего процесса
            last_loaded_ts = wf_setting[self.LAST_LOADED_TS_KEY]  # Извлекаем время последней загрузки

            restaurants_to_load = self.get_restaurants_from_stg(last_loaded_ts)  # Получаем пользователей из STG
            if not restaurants_to_load:
                self.log.info("No new restaurants found.")  # Логируем отсутствие новых пользователей
                return

            for restaurant_data in restaurants_to_load:
                self.dest.insert_restaurant(conn, restaurant_data)  # Вставляем каждого пользователя в базу данных

            # Сохраняем прогресс
            new_last_loaded_ts = max(restaurant_data["update_ts"] for restaurant_data in restaurants_to_load)  # Получаем новое время последней загрузки
            self.save_last_loaded_ts(conn, new_last_loaded_ts)  # Сохраняем новое время последней загрузки

            self.log.info(f"Loaded {len(restaurants_to_load)} restaurants.")  # Логируем количество загруженных пользователей

    def get_restaurants_from_stg(self, last_loaded_ts: datetime) -> List[Dict[str, Any]]:
        # Метод для получения пользователей из STG, обновленных после last_loaded_ts
        with self.pg_dest.client() as conn:  # Используем клиент для соединения с БД
            cur = conn.cursor()  # Получаем курсор от соединения
            cur.execute(
                """
                SELECT object_value
                FROM stg.ordersystem_restaurants
                WHERE update_ts > %(last_loaded_ts)s
                ORDER BY update_ts ASC
                LIMIT %(limit)s;
                """,
                {
                    "last_loaded_ts": last_loaded_ts,  # Используем время последней загрузки
                    "limit": self.BATCH_LIMIT  # Устанавливаем лимит на количество загружаемых пользователей
                }
            )
            objs = cur.fetchall()  # Получаем все результаты запроса
            return [str2json(obj[0]) for obj in objs]  # Преобразуем результаты в JSON

    def get_last_loaded_ts(self, conn: Connection) -> dict:
        # Метод для получения времени последней загрузки из базы данных
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
            return {self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()}  # Возвращаем значение по умолчанию.

    def save_last_loaded_ts(self, conn: Connection, last_loaded_ts: datetime) -> None:
        # Метод для сохранения времени последней загрузки в базе данных
        if isinstance(last_loaded_ts, str):  # Проверяем, является ли last_loaded_ts строкой
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts)  # Преобразуем строку в объект datetime

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
                    "wf_setting": json.dumps({"last_loaded_ts": last_loaded_ts.isoformat()})  # Сериализация в JSON
                }
            )
