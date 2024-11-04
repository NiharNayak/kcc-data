{{ config(
    materialized='table',
    tags=['marts', 'products'],
    clustered_by=['product_category', 'product_id']
) }}

with products as (
    select * from {{ ref('stg_products') }}
),

product_sales as (
    select
        product_id,
        count(*) as total_transactions,
        sum(quantity_sold) as total_units_sold,
        sum(total_amount) as total_revenue,
        avg(price_per_unit) as avg_selling_price
    from {{ ref('stg_sales') }}
    group by product_id
),

product_metrics as (
    select
        p.*,
        coalesce(ps.total_transactions, 0) as total_transactions,
        coalesce(ps.total_units_sold, 0) as total_units_sold,
        coalesce(ps.total_revenue, 0) as total_revenue,
        coalesce(ps.avg_selling_price, p.original_price) as avg_selling_price,
        case
            when ps.avg_selling_price > p.original_price then true
            else false
        end as is_price_increased,
        coalesce((ps.avg_selling_price - p.original_price) / p.original_price * 100, 0) as price_change_percentage
    from products p
    left join product_sales ps on p.product_id = ps.product_id
)

select * from product_metrics