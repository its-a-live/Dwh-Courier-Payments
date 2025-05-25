from lib import PgConnect

class CourierLedgerRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def load_courier_ledger(self) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
;WITH orders_sums AS 
(
    SELECT 
        ord.id AS order_id,
        ord.delivery_id,
        ord.timestamp_id,
        SUM(total_sum) AS order_total_sum
    FROM 
        dds.fct_product_sales ps
    INNER JOIN 
        dds.dm_orders ord ON ps.order_id = ord.id
    WHERE 
        ord.order_status = 'CLOSED'
        AND delivery_id IS NOT NULL
    GROUP BY 
        ord.id, ord.timestamp_id
),

courier_rates AS
(
    SELECT 
        dc.id AS courier_id,
        dc.courier_name,
        tss.year AS settlement_year,
        tss.month AS settlement_month,
        COUNT(DISTINCT os.order_id) AS orders_count,
        SUM(os.order_total_sum) AS orders_total_sum,
        SUM(os.order_total_sum) * 0.25 AS order_processing_fee,
        ROUND(AVG(dd.rate::numeric), 2) AS rate_avg,
        SUM(dd.tip_sum) AS courier_tips_sum
    FROM 
        orders_sums os
    INNER JOIN 
        dds.dm_deliveries dd ON os.delivery_id = dd.id
    INNER JOIN 
        dds.dm_couriers dc ON dd.courier_id = dc.id
    INNER JOIN 
        dds.dm_timestamps AS tss ON os.timestamp_id = tss.id
    GROUP BY 
        dc.id, dc.courier_name, tss.year, tss.month
)

INSERT INTO cdm.dm_courier_ledger(
    courier_id,
    courier_name,
    settlement_year,
    settlement_month,
    orders_count,
    orders_total_sum,
    order_processing_fee,
    courier_reward_sum,
    courier_tips_sum
)
SELECT 
    cr.courier_id,
    cr.courier_name,
    cr.settlement_year,
    cr.settlement_month,
    cr.orders_count,
    cr.orders_total_sum,
    cr.order_processing_fee,
    
    -- Расчет суммы выплаты курьеру
    SUM(
        CASE
            WHEN cr.rate_avg < 4 THEN
                GREATEST(cr.orders_total_sum * 0.05, 100)
            WHEN cr.rate_avg >= 4 AND cr.rate_avg < 4.5 THEN
                GREATEST(cr.orders_total_sum * 0.07, 150)
            WHEN cr.rate_avg >= 4.5 AND cr.rate_avg < 4.9 THEN
                GREATEST(cr.orders_total_sum * 0.08, 175)
            WHEN cr.rate_avg >= 4.9 THEN
                GREATEST(cr.orders_total_sum * 0.10, 200)
        END
    ) + cr.courier_tips_sum * 0.95 AS courier_reward_sum,
    
    cr.courier_tips_sum

FROM 
    courier_rates cr
GROUP BY 
    cr.courier_id, 
    cr.courier_name, 
    cr.settlement_year, 
    cr.settlement_month, 
    cr.orders_count, 
    cr.orders_total_sum, 
    cr.order_processing_fee,
    cr.courier_tips_sum
ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
SET
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    order_processing_fee = EXCLUDED.order_processing_fee,
    courier_reward_sum = EXCLUDED.courier_reward_sum,
    courier_tips_sum = EXCLUDED.courier_tips_sum;
                    """
                )
                conn.commit()

class CourierLedgerLoader:
    def __init__(self, pg: PgConnect) -> None:
        self.repository = CourierLedgerRepository(pg)

    def load_courier_ledger(self):
        self.repository.load_courier_ledger()
