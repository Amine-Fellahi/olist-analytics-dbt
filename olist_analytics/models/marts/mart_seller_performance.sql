WITH sellers_metrics AS(
    SELECT seller_id, seller_city, seller_state, total_revenue, total_orders, avg_review_score
    FROM {{ref('int_sellers_enriched')}}
)
SELECT seller_id, seller_city, seller_state, total_revenue, total_orders, avg_review_score
FROM sellers_metrics
ORDER BY total_revenue DESC