{{ config(
    materialized='view',
    tags=['sales', 'daily']
) }}

with source as (
    select * from {{ ref('sales') }}
),

transformed as (
    select
        -- IDs and core fields
        trim(Transaction_ID) as transaction_id,
        trim(Customer_ID) as customer_id,
        trim(Product_ID) as product_id,
        cast(Quantity_Sold as integer) as quantity_sold,
        cast(Price_per_Unit as numeric) as price_per_unit,

        -- Calculated fields
        cast(Quantity_Sold as integer) * cast(Price_per_Unit as numeric) as total_amount,

        -- Date and time
        to_date(Transaction_Date, '%m/%d/%y') as transaction_date,

        -- Additional fields
        trim(Salesperson_ID) as salesperson_id,
        trim(Payment_Method) as payment_method,
        trim(Store_ID) as store_id,

        -- Payment method flags
        payment_method = 'Bank Transfer' as is_bank_transfer,
        payment_method = 'Cash' as is_cash,

        -- Metadata
        current_timestamp as dbt_updated_at
    from source
)

select * from transformed