select
    customer_id as customer_key,
    customer_name,
    age_group,
    gender,
    city,
    state,
    membership_type,
    customer_segment,
    annual_income_group,
    signup_date
from {{ ref('stg_customers') }}
