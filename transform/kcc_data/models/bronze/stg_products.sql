{{ config(
    materialized='view',
    tags=['products', 'reference']
) }}

with source as (
    select * from {{ ref('products') }}
),

transformed as (
    select
        -- IDs and core fields
        trim(Product_ID) as product_id,
        trim(Product_Name) as product_name,
        trim(Product_Category) as product_category,
        trim(Supplier_ID) as supplier_id,
        cast(Orig_Price as numeric) as original_price,

        
        -- Price categorization
        case
            when cast(Orig_Price as numeric) < 100 then 'Budget'
            when cast(Orig_Price as numeric) < 300 then 'Mid-Range'
            when cast(Orig_Price as numeric) < 500 then 'Premium'
            else 'Luxury'
        end as price_tier,
        
        -- Metadata
        current_timestamp as dbt_updated_at
    from source
),

with_rankings as (
    select 
        *,
        row_number() over (partition by product_category order by original_price desc) as price_rank_in_category,
        percent_rank() over (partition by product_category order by original_price) as price_percentile
    from transformed
)

select * from with_rankings