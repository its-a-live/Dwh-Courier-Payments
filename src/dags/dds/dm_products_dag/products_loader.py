from logging import Logger
from datetime import datetime
from lib.dict_util import str2json
from lib import PgConnect
from typing import Any, List, Dict
from psycopg import Connection
import json

class ProductsDestRepository:
    def insert_product(self, conn: Connection, product_data: dict, restaurant_id: str) -> None:
        with conn.cursor() as cur:
            # Сначала закрываем предыдущие записи, если имя продукта изменилось
            cur.execute(
                """
                UPDATE dds.dm_products 
                SET active_to = %(update_ts)s
                WHERE product_id = %(id)s
                AND active_to = '2099-12-31 00:00:00'
                AND product_name != %(name)s;
                """,
                {
                    "id": product_data["_id"],  # id продукта
                    "name": product_data["name"],  # новое имя продукта
                    "update_ts": product_data["update_ts"]  # метка времени
                }
            )

            # Затем вставляем новую запись с актуальной информацией
            cur.execute(
                """
                INSERT INTO dds.dm_products (product_id, product_name, product_price, restaurant_id, active_from, active_to)
                VALUES (%(id)s, %(name)s, %(price)s, %(restaurant_id)s, %(update_ts)s, '2099-12-31 00:00:00')
                ON CONFLICT (product_id) DO NOTHING;
                """,
                {
                    "id": product_data["_id"],  # id продукта
                    "name": product_data["name"],  # имя продукта
                    "price": product_data["price"],  # цена продукта
                    "restaurant_id": restaurant_id,  # id ресторана
                    "update_ts": product_data["update_ts"]  # метка времени
                }
            )

class ProductsLoader:
    WF_KEY = "dm_products_load_workflow"  # Ключ для рабочего процесса загрузки продуктов
    LAST_LOADED_TS_KEY = "last_loaded_ts"  # Ключ для хранения времени последней загрузки
    BATCH_LIMIT = 100  # Лимит на количество загружаемых продуктов за раз

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.dest = ProductsDestRepository()  # Создаем экземпляр репозитория продуктов
        self.log = log  # Устанавливаем логгер

    def load_products(self) -> None:
        with self.pg_dest.client() as conn:  # Используем клиент для соединения с БД
            wf_setting = self.get_last_loaded_ts(conn)  # Получаем последние настройки рабочего процесса
            last_loaded_ts = wf_setting[self.LAST_LOADED_TS_KEY]  # Извлекаем время последней загрузки

            products_to_load = self.get_products_from_stg(last_loaded_ts)  # Получаем продукты из STG
            
            if not products_to_load:
                self.log.info("No new products found.")  # Логируем отсутствие новых продуктов
                return

            for restaurant_data in products_to_load:
                restaurant_id = self.get_restaurant_id(conn, restaurant_data["_id"])  # Получаем id ресторана по его _id
                for product in restaurant_data["menu"]:
                    product["update_ts"] = restaurant_data["update_ts"]  # Добавляем метку времени продукта
                    
                    self.dest.insert_product(conn, product, restaurant_id)  # Вставляем продукт в базу данных

            # Сохраняем прогресс
            new_last_loaded_ts = max(restaurant_data["update_ts"] for restaurant_data in products_to_load)  # Получаем новое время последней загрузки
            self.save_last_loaded_ts(conn, new_last_loaded_ts)  # Сохраняем новое время последней загрузки

            self.log.info(f"Loaded products for {len(products_to_load)} restaurants.")  # Логируем количество загруженных ресторанов

    def get_products_from_stg(self, last_loaded_ts: datetime) -> List[Dict[str, Any]]:
        # Метод для получения продуктов из STG, обновленных после last_loaded_ts
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
                    "limit": self.BATCH_LIMIT  # Устанавливаем лимит на количество загружаемых продуктов
                }
            )
            objs = cur.fetchall()  # Получаем все результаты запроса
            return [str2json(obj[0]) for obj in objs]  # Преобразуем результаты в JSON

    def get_restaurant_id(self, conn: Connection, restaurant_id: str) -> str:
        # Метод для получения id ресторана по его _id
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
                return result[0]  # Возвращаем id ресторана
            raise ValueError(f"Restaurant with id {restaurant_id} not found.")  # Обрабатываем случай, когда ресторан не найден

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
