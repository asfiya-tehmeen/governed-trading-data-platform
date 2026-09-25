-- One row per Deriv tick; re-deliveries removed (first arrival wins).
with source as (
    select * from {{ source('raw', 'ticks') }}
)

select
    tick_id,
    symbol,
    quote,
    bid,
    ask,
    pip_size,
    quoted_at,
    date(quoted_at) as quote_date,
    ingested_at
from source
qualify row_number() over (partition by tick_id order by ingested_at) = 1
