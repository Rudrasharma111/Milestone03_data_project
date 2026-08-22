select
    customer_id,
    trim(customer_name) as customer_name,
    age_group,
    gender,
    trim(city) as city,
    trim(state) as state,
    membership_type,
    customer_segment,
    annual_income_group,
    signup_date
from {{ source('raw', 'customers') }}
