-- One row per executed trade.
select
    trade_id,
    client_id,
    symbol,
    trade_date,
    executed_at,
    side,
    quantity,
    signed_quantity,
    price,
    notional,
    commission,
    tick_id,
    ingested_at
from {{ ref('stg_trades') }}
