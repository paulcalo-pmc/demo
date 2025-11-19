{{ config(
    materialized='table',
    tags=['mart', 'dimension']
) }}

select
    item_id,
    item_code,
    category,
    current_price
from {{ ref('stg_tpcds_item') }}
