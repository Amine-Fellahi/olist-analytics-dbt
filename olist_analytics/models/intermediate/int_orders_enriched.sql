WITH orders AS(
    SELECT *
    FROM {{ ref('stg_orders') }}
),
items AS (
    SELECT COUNT(*) AS item_count, SUM(price) AS total_revenue, SUM(freight_value) AS total_freight_value, order_id
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
),
payment_counts AS (
    SELECT
        order_id,
        payment_type,
        COUNT(*) AS payment_count
    FROM {{ ref('stg_order_payments') }}
    GROUP BY order_id, payment_type
),
ranked_payments AS (
    SELECT
        order_id,
        payment_type,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY payment_count DESC) AS row_num
    FROM payment_counts
),
common_payment AS (
    SELECT
        order_id,
        payment_type AS common_payment_type
    FROM ranked_payments
    WHERE row_num = 1
),
total_payments AS (
    SELECT
        order_id,
        SUM(payment_value) AS total_payments
    FROM {{ ref('stg_order_payments') }}
    GROUP BY order_id
),
reviews AS (
    SELECT AVG(review_score) AS avg_review_score, order_id
    FROM {{ ref('stg_order_reviews') }}
    GROUP BY order_id
),
customers AS (
    SELECT customer_id, customer_state, customer_city
    FROM {{ ref('stg_customers') }}
)
SELECT
    o.order_id,
    o.customer_id,
    o.ordered_at,
    o.delivered_at,
    o.estimated_delivery_at,
    i.total_revenue,
    i.item_count,
    i.total_freight_value,
    t.total_payments,
    p.common_payment_type,
    r.avg_review_score,
    c.customer_city,
    c.customer_state
FROM orders o
LEFT JOIN items i ON o.order_id=i.order_id
LEFT JOIN total_payments t ON o.order_id=t.order_id
LEFT JOIN common_payment p ON o.order_id=p.order_id
LEFT JOIN reviews r ON o.order_id=r.order_id
LEFT JOIN customers c ON o.customer_id=c.customer_id