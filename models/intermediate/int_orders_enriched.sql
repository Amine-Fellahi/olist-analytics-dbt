WITH orders AS(
    SELECT *
    FROM {{ ref('stg_orders') }}
),
items AS ( 
    SELECT COUNT(*) AS item_count, SUM(price) AS total_revenue, SUM(freight_value) AS total_freight_value, order_id
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id),
payments AS (
    SELECT SUM(payment_value) AS total_payments, MAX(payment_type) AS common_payment_type, order_id
    FROM {{ ref('stg_order_payments') }}
    GROUP BY order_id),
reviews AS (
    SELECT AVG (review_score) AS avg_review_score, order_id 
FROM {{ref('stg_order_reviews')}}
GROUP BY order_id),
customers AS (
    SELECT customer_id, customer_state, customer_city
FROM {{ref('stg_customers')}})
SELECT
    o.order_id,
    o.customer_id,
    o.ordered_at,
    o.delivered_at,
    o.estimated_delivery_at,
    i.total_revenue,
    i.item_count,
    i.total_freight_value,
    p.total_payments,
    p.common_payment_type,
    r.avg_review_score,
    c.customer_city,
    c.customer_state
FROM orders o
LEFT JOIN items i ON o.order_id=i.order_id
LEFT JOIN payments p ON o.order_id=p.order_id
LEFT JOIN reviews r ON o.order_id=r.order_id
LEFT JOIN customers c ON o.customer_id=c.customer_id