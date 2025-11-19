{{ config(
    materialized='view',
    tags=['staging']
) }}

select
    i_item_sk as item_id,
    i_item_id as item_code,
    i_category as category,
    i_current_price as current_price
from {{ source('tpcds', 'ITEM') }}
