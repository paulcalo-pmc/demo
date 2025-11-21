select *
from {{ ref('fact_sales') }}
where quantity > 100