# Olist Analytics

A dbt analytics project built on the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). It transforms raw transactional data into clean, tested, and documented analytics-ready tables in BigQuery.

## Tech Stack

- **dbt** — data modeling and transformation
- **Google BigQuery** — data warehouse
- **Git** — version control

## Architecture

```
Raw Sources (BigQuery)
        |
   Staging Layer       → light cleaning, one model per source table (views)
        |
 Intermediate Layer    → joins and aggregations, one row per entity (views)
        |
    Marts Layer        → business-ready tables for dashboards and reporting (tables)
```

## Models

| Model | Layer | Description |
|---|---|---|
| stg_customers | Staging | Customer IDs and location |
| stg_orders | Staging | Orders with timestamps (delivered only) |
| stg_order_items | Staging | One row per item per order |
| stg_order_payments | Staging | One row per payment entry |
| stg_order_reviews | Staging | Customer review scores |
| stg_sellers | Staging | Seller IDs and location |
| stg_products | Staging | Product IDs and categories |
| int_orders_enriched | Intermediate | One row per order with revenue, payments, reviews, and customer info |
| int_sellers_enriched | Intermediate | One row per seller with revenue, order count, and avg review score |
| int_products_enriched | Intermediate | One row per product with revenue, order count, and avg review score |
| mart_sales_overview | Mart | Monthly revenue and reviews aggregated by customer state |
| mart_seller_performance | Mart | Seller-level performance metrics |
| mart_product_category | Mart | Revenue and reviews aggregated by product category |

## Project Structure

```
olist_analytics/
├── dbt_project.yml
└── models/
    ├── staging/
    │   ├── schema.yml
    │   └── stg_*.sql
    ├── intermediate/
    │   ├── schema.yml
    │   └── int_*.sql
    └── marts/
        ├── schema.yml
        └── mart_*.sql
```

## How to Run

```bash
# Run all models
dbt run

# Run tests
dbt test

# Generate and serve documentation
dbt docs generate
dbt docs serve
```
