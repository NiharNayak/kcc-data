{{ config(
    materialized='view',
    tags=['stores', 'reference']
) }}

with source as (
    select * from {{ ref('stores') }}
),

transformed as (
    select
        -- IDs and core fields
        trim(Store_ID) as store_id,
        trim(Store_Name) as store_name,
        trim(Region) as region,
        trim(Manager_ID) as manager_id,
        trim(Manager_Name) as manager_name,

        -- Derived fields
        case
            when Region = 'Midwest' then 'Central'
            when Region in ('Northeast', 'Southeast') then 'Eastern'
            when Region in ('Southwest', 'West') then 'Western'
            else 'Other'
        end as region_category,

        -- Region flags
        Region = 'Midwest' as is_midwest,

        -- Metadata
        current_timestamp as dbt_updated_at
    from source
),

final as (
    select
        *,
        count(*) over (partition by region) as stores_in_region
    from transformed
)

select * from final