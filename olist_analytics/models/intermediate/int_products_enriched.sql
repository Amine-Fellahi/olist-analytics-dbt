WITH products AS (
    SELECT product_id, product_category_name
    FROM {{ ref('stg_products') }}
),
order_items AS (
    SELECT product_id, SUM(price) AS total_revenue, COUNT(DISTINCT order_id) AS total_orders
    FROM {{ ref('stg_order_items') }}
    GROUP BY product_id
),
reviews AS (
    SELECT AVG(r.review_score) AS avg_review_score, oi.product_id
    FROM {{ ref('stg_order_reviews') }} r
    LEFT JOIN {{ ref('stg_order_items') }} oi
    ON r.order_id = oi.order_id
    GROUP BY product_id
)
SELECT
    p.product_id,
    p.product_category_name,
    oi.total_revenue,
    oi.total_orders,
    r.avg_review_score
FROM products p
LEFT JOIN order_items oi ON p.product_id = oi.product_id
LEFT JOIN reviews r ON p.product_id = r.product_id
