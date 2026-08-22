select
    campaign_id as campaign_key,
    campaign_name,
    expected_performance
from {{ ref('stg_campaigns') }}
