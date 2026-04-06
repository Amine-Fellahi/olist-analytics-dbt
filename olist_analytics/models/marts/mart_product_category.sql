WITH products_enriched AS (
    SELECT product_category_name, total_revenue, total_orders, avg_review_score
    FROM {{ ref('int_products_enriched') }}
)
SELECT
    product_category_name,
    SUM(total_revenue) AS total_revenue,
    SUM(total_orders) AS total_orders,
    AVG(avg_review_score) AS avg_review_score
FROM products_enriched
GROUP BY product_category_name
ORDER BY total_revenue DESC
