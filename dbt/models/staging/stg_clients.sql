-- Current state of each client: the latest snapshot by updated_at.
with source as (
    select * from {{ source('raw', 'clients') }}
)

select
    client_id,
    full_name,
    lower(trim(email)) as email,
    upper(country) as country,
    account_currency,
    risk_tier,
    opened_at,
    updated_at
from source
qualify row_number() over (partition by client_id order by updated_at desc, ingested_at desc) = 1
