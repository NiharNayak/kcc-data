{{ config(
    materialized='incremental',
    unique_key=['transaction_id', 'transaction_date'],
    tags=['marts', 'sales', 'core', 'daily'],
    partition_by={
      "field": "transaction_date",
      "data_type": "date",
      "granularity": "day"
    },
    clustered_by=['product_id', 'store_id', 'customer_id']
) }}

with sales as (
    select * from {{ ref('stg_sales') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

enriched_sales as (
    select
        -- Transaction keys and dates
        s.transaction_id,
        s.transaction_date,
        date_trunc('month', s.transaction_date) as transaction_month,

        -- Foreign keys
        s.customer_id,
        s.product_id,
        s.store_id,
        s.salesperson_id,

        -- Transaction measures
        s.quantity_sold,
        s.price_per_unit,
        s.total_amount,

        -- Price analysis
        p.original_price as product_original_price,
        p.product_category,
        s.price_per_unit - p.original_price as price_difference,
        (s.price_per_unit - p.original_price) / p.original_price * 100 as price_difference_pct,

        -- Payment details
        s.payment_method,
        s.is_bank_transfer,
        s.is_cash,

        -- Metadata
        s.dbt_updated_at

    from sales s
    left join products p
    on
        s.product_id = p.product_id
)

select * from enriched_sales