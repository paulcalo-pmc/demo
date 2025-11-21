{{ config(
    materialized='table',
    tags=['mart', 'fact']
) }}

-- Find the first real month in the dataset
with valid_dates as (
    select
        d_date_sk as date_key,
        d_date as date_actual,
        d_year,
        d_moy,
        row_number() over (order by d_date) as rn
    from {{ source('tpcds', 'DATE_DIM') }}
    where d_date_sk in (select distinct ss_sold_date_sk from {{ source('tpcds', 'STORE_SALES') }})
),

first_month as (
    -- pick the earliest month with sales
    select
        d_year,
        d_moy
    from valid_dates
    where rn = 1
),

sales as (
    select ss.*
    from {{ ref('stg_tpcds_store_sales') }} ss
    join valid_dates vd
      on ss.date_key = vd.date_key
    join first_month fm
      on vd.d_year = fm.d_year
     and vd.d_moy = fm.d_moy
),

items as (
    select * from {{ ref('dim_item') }}
)

select
    s.date_key,
    s.customer_id,
    s.item_id,
    s.quantity,
    s.sales_price,
    s.quantity * s.sales_price as revenue,

    -- NEW FIELD USING THE MACRO
    {{ calc_revenue('s.quantity', 's.sales_price') }} as revenue_from_macro,

    i.category
from sales s
left join items i
  on s.item_id = i.item_id