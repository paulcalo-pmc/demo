{% macro calc_revenue(quantity, sales_price) %}
    {{ quantity }} * {{ sales_price }}
{% endmacro %}