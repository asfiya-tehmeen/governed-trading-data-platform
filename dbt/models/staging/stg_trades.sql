-- One row per trade. Pub/Sub delivery is at-least-once, so the same trade_id can
-- arrive more than once; keep the first arrival. Conflicting duplicates (same id,
-- different content) are caught and failed by the duplicates quality check.
with source as (
    select * from {{ source('raw', 'trades') }}
)

select
    trade_id,
    client_id,
    symbol,
    side,
    quantity,
    price,
    commission,
    case side when 'BUY' then quantity when 'SELL' then -quantity end as signed_quantity,
    quantity * price as notional,
    executed_at,
    date(executed_at) as trade_date,
    tick_id,
    source,
    ingested_at
from source
qualify row_number() over (partition by trade_id order by ingested_at) = 1
