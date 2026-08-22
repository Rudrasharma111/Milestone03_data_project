select
    campaign_id,
    trim(campaign_name) as campaign_name,
    expected_performance
from {{ source('raw', 'campaigns') }}
