{{ config(
    materialized='view',
    tags=['staging']
) }}

select
    ss_sold_date_sk as date_key,
    ss_customer_sk as customer_id,
    ss_item_sk as item_id,
    ss_quantity as quantity,
    ss_list_price as list_price,
    ss_sales_price as sales_price
from {{ source('tpcds', 'STORE_SALES') }}
