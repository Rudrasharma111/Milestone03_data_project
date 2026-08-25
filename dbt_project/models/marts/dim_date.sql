with date_spine as (
    select date_add(date('2015-01-01'), interval x day) as full_date
    from unnest(generate_array(0, 6000)) as x
)

select
full_date as date_key,
extract(year from full_date) as year,
extract(month from full_date) as month,
extract(day from full_date) as day,
extract(quarter from full_date) as quarter,
extract(dayofweek from full_date) as day_of_week,
format_date('%A', full_date) as day_name,
format_date('%B', full_date) as month_name
from date_spine