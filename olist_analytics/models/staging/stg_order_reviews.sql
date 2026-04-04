with source as (
    select * from {{ source('raw', 'order_reviews_clean') }}
),

renamed as (
    select
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message
    from source
)

select * from renamed