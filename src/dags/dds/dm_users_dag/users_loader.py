from logging import Logger
from datetime import datetime
from lib.dict_util import str2json
from lib import PgConnect
from typing import Any, List, Dict 
from psycopg import Connection 
import json

class UsersDestRepository:
    def insert_user(self, conn: Connection, user_data: dict) -> None:
        # Метод для вставки или обновления пользователя в базе данных
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.dm_users (user_id, user_name, user_login)
                VALUES (%(id)s, %(name)s, %(login)s)
                ON CONFLICT (user_id) DO UPDATE
                SET 
                    user_name = EXCLUDED.user_name,
                    user_login = EXCLUDED.user_login;
                """,
                {
                    "id": user_data["_id"],  # Получаем id пользователя
                    "name": user_data["name"],  # Получаем имя пользователя
                    "login": user_data["login"]  # Получаем логин пользователя
                }
            )

class UsersLoader:
    WF_KEY = "dm_users_load_workflow"  # Ключ для рабочего процесса загрузки пользователей
    LAST_LOADED_TS_KEY = "last_loaded_ts"  # Ключ для хранения времени последней загрузки
    BATCH_LIMIT = 100  # Лимит на количество загружаемых пользователей за раз

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        # Инициализация класса с соединением к PostgreSQL и логгером
        self.pg_dest = pg_dest
        self.dest = UsersDestRepository()  # Создаем экземпляр репозитория пользователей
        self.log = log  # Устанавливаем логгер

    def load_users(self) -> None:
        # Метод для загрузки пользователей из источника данных
        with self.pg_dest.client() as conn:  # Используем клиент для соединения с БД
            wf_setting = self.get_last_loaded_ts(conn)  # Получаем последние настройки рабочего процесса
            last_loaded_ts = wf_setting[self.LAST_LOADED_TS_KEY]  # Извлекаем время последней загрузки

            users_to_load = self.get_users_from_stg(last_loaded_ts)  # Получаем пользователей из STG
            if not users_to_load:
                self.log.info("No new users found.")  # Логируем отсутствие новых пользователей
                return

            for user_data in users_to_load:
                self.dest.insert_user(conn, user_data)  # Вставляем каждого пользователя в базу данных

            # Сохраняем прогресс
            new_last_loaded_ts = max(user_data["update_ts"] for user_data in users_to_load)  # Получаем новое время последней загрузки
            self.save_last_loaded_ts(conn, new_last_loaded_ts)  # Сохраняем новое время последней загрузки

            self.log.info(f"Loaded {len(users_to_load)} users.")  # Логируем количество загруженных пользователей

    def get_users_from_stg(self, last_loaded_ts: datetime) -> List[Dict[str, Any]]:
        # Метод для получения пользователей из STG, обновленных после last_loaded_ts
        with self.pg_dest.client() as conn:  # Используем клиент для соединения с БД
            cur = conn.cursor()  # Получаем курсор от соединения
            cur.execute(
                """
                SELECT object_value
                FROM stg.ordersystem_users
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
