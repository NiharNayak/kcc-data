{{ config(
    materialized='table',
    tags=['marts', 'sales', 'reporting'],
    partition_by={
      "field": "transaction_date",
      "data_type": "date",
      "granularity": "day"
    },
    clustered_by=['product_category', 'store_name', 'region', 'product_name']
) }}

with daily_metrics as (
    select
        transaction_date,
        store_id,
        product_id,
        product_category,
        count(*) as total_transactions,
        sum(quantity_sold) as total_units,
        sum(total_amount) as total_revenue,
        count(distinct customer_id) as unique_customers,
        count(distinct product_id) as unique_products,
        avg(total_amount) as avg_transaction_value,
        sum(case when is_bank_transfer then total_amount else 0 end) as bank_transfer_revenue,
        sum(case when is_cash then total_amount else 0 end) as cash_revenue,
        avg(price_per_unit) as avg_selling_price,
        max(price_per_unit) as max_selling_price,
        min(price_per_unit) as min_selling_price
    from {{ ref('mart_fct_sales') }}
    group by 1,2,3,4
),

product_metrics as (
    select
        product_id,
        transaction_date,
        sum(quantity_sold) as total_units_sold,
        sum(total_amount) as total_revenue,
        avg(price_per_unit) as avg_selling_price,
        count(distinct store_id) as selling_stores,
        count(distinct customer_id) as unique_customers,
        -- Product mix metrics
        sum(quantity_sold) / nullif(sum(sum(quantity_sold)) over(partition by transaction_date), 0) * 100 as daily_quantity_share,
        sum(total_amount) / nullif(sum(sum(total_amount)) over(partition by transaction_date), 0) * 100 as daily_revenue_share,
        -- Price metrics
        avg(price_per_unit - product_original_price) as avg_price_deviation,
        avg((price_per_unit - product_original_price) / product_original_price * 100) as avg_price_deviation_pct
    from {{ ref('mart_fct_sales') }}
    group by 1,2
),

category_metrics as (
    select
        transaction_date,
        product_category,
        count(distinct product_id) as category_unique_products,
        sum(quantity_sold) as category_units_sold,
        sum(total_amount) as category_revenue,
        avg(price_per_unit) as category_avg_price,
        count(distinct store_id) as category_selling_stores,
        -- Category share calculations
        sum(total_amount) / nullif(sum(sum(total_amount)) over(partition by transaction_date), 0) * 100 as category_revenue_share
    from {{ ref('mart_fct_sales') }}
    group by 1,2
),

product_store_metrics as (
    select
        store_id,
        product_id,
        transaction_date,
        sum(quantity_sold) as store_product_units,
        sum(total_amount) as store_product_revenue,
        avg(price_per_unit) as store_product_avg_price,
        count(distinct customer_id) as store_product_customers
    from {{ ref('mart_fct_sales') }}
    group by 1,2,3
),

final as (
    select
        -- Date and Store Dimensions
        dm.transaction_date,
        dm.store_id,
        s.store_name,
        s.region,
        s.region_category,

        -- Product Dimensions
        dm.product_id,
        p.product_name,
        p.product_category,
        p.price_tier,

        -- Daily Store-Product Metrics
        dm.total_transactions,
        dm.total_units,
        dm.total_revenue,
        dm.unique_customers,
        dm.avg_transaction_value,
        dm.bank_transfer_revenue,
        dm.cash_revenue,
        dm.avg_selling_price,
        dm.max_selling_price,
        dm.min_selling_price,

        -- Product Overall Metrics
        pm.total_units_sold as product_total_units,
        pm.total_revenue as product_total_revenue,
        pm.avg_selling_price as product_avg_price,
        pm.selling_stores as product_selling_stores,
        pm.unique_customers as product_unique_customers,
        pm.daily_quantity_share,
        pm.daily_revenue_share,
        pm.avg_price_deviation,
        pm.avg_price_deviation_pct,

        -- Category Metrics
        cm.category_unique_products,
        cm.category_units_sold,
        cm.category_revenue,
        cm.category_avg_price,
        cm.category_selling_stores,
        cm.category_revenue_share,

        -- Store-Product Specific Metrics
        psm.store_product_units,
        psm.store_product_revenue,
        psm.store_product_avg_price,
        psm.store_product_customers,

        -- Store Rankings
        row_number() over(partition by dm.transaction_date order by dm.total_revenue desc) as daily_store_revenue_rank,
        percent_rank() over(partition by dm.transaction_date order by dm.total_revenue) as daily_store_revenue_percentile,

        -- Product Rankings
        row_number() over(partition by dm.transaction_date order by pm.total_revenue desc) as daily_product_revenue_rank,
        percent_rank() over(partition by dm.transaction_date order by pm.total_revenue) as daily_product_revenue_percentile,

        -- Regional Aggregates
        sum(dm.total_revenue) over(partition by s.region, dm.transaction_date) as region_daily_revenue,
        avg(dm.total_revenue) over(partition by s.region, dm.transaction_date) as region_daily_avg_revenue,

        -- Product Mix Analysis
        sum(dm.total_revenue) over(partition by p.product_category, dm.transaction_date) as category_daily_revenue,
        avg(dm.total_revenue) over(partition by p.product_category, dm.transaction_date) as category_daily_avg_revenue,

        -- Period over Period Calculations
        lag(dm.total_revenue) over(partition by dm.store_id, dm.product_id order by dm.transaction_date) as prev_day_revenue,
        dm.total_revenue - lag(dm.total_revenue) over(partition by dm.store_id, dm.product_id order by dm.transaction_date) as revenue_day_change,

        -- Running Totals and Averages
        sum(dm.total_revenue) over(partition by dm.store_id, dm.product_id order by dm.transaction_date) as cumulative_revenue,
        avg(dm.total_revenue) over(partition by dm.store_id, dm.product_id order by dm.transaction_date rows between 6 preceding and current row) as rolling_7day_avg_revenue,

        -- Price Trend Analysis
        avg(dm.avg_selling_price) over(partition by dm.product_id order by dm.transaction_date rows between 6 preceding and current row) as rolling_7day_avg_price,
        stddev(dm.avg_selling_price) over(partition by dm.product_id order by dm.transaction_date rows between 6 preceding and current row) as rolling_7day_price_stddev

    from daily_metrics dm
    left join {{ ref('stg_stores') }} s on dm.store_id = s.store_id
    left join {{ ref('stg_products') }} p on dm.product_id = p.product_id
    left join product_metrics pm on dm.product_id = pm.product_id and dm.transaction_date = pm.transaction_date
    left join category_metrics cm on dm.product_category = cm.product_category and dm.transaction_date = cm.transaction_date
    left join product_store_metrics psm on dm.store_id = psm.store_id
        and dm.product_id = psm.product_id
        and dm.transaction_date = psm.transaction_date
)

select * from final