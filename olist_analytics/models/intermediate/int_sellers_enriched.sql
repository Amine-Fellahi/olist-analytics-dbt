WITH sellers AS(
    SELECT seller_id, seller_city, seller_state 
    FROM {{ ref('stg_sellers') }}
),
orders_items AS(
    SELECT oi.seller_id, SUM(oi.price) AS total_revenue, COUNT(DISTINCT order_id) AS total_orders
    FROM {{ ref('stg_order_items') }} oi
    GROUP BY seller_id
),
review AS(
    SELECT AVG(r.review_score) AS avg_review_score, oi.seller_id
    FROM {{ref('stg_order_reviews')}} r
    LEFT JOIN {{ref('stg_order_items')}} oi 
    ON r.order_id=oi.order_id
    GROUP by seller_id
)
SELECT
    s.seller_id,
    s.seller_city,
    s.seller_state,
    oi.total_revenue,
    oi.total_orders,
    r.avg_review_score
FROM sellers s
LEFT JOIN orders_items oi ON s.seller_id=oi.seller_id
LEFT JOIN review r ON r.seller_id=s.seller_id