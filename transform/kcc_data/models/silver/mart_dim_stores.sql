{{ config(
    materialized='table',
    tags=['marts', 'products'],
    clustered_by=['region', 'store_name']
) }}

with stores as (
    select * from {{ ref('stg_stores') }}
),

store_metrics as (
    select
        store_id,
        count(*) as total_transactions,
        sum(total_amount) as total_revenue
    from {{ ref('stg_sales') }}
    group by store_id
)

select
    s.*,
    coalesce(sm.total_transactions, 0) as total_transactions,
    coalesce(sm.total_revenue, 0) as total_revenue
from stores s
left join store_metrics sm on s.store_id = sm.store_id