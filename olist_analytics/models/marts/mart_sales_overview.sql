WITH orders_enriched AS(
    SELECT order_id, total_revenue, avg_review_score, customer_state, ordered_at
    FROM {{ ref('int_orders_enriched') }}
)
SELECT customer_state, SUM(total_revenue) AS total_revenue, AVG(avg_review_score) AS avg_review_score, COUNT(DISTINCT order_id) AS total_orders, DATE_TRUNC(ordered_at, MONTH) AS ordered_at
FROM orders_enriched
GROUP BY customer_state, DATE_TRUNC(ordered_at, MONTH)
ORDER BY ordered_at, customer_state