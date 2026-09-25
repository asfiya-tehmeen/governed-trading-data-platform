-- Current open exposure per client and symbol, marked to the latest live tick.
-- Flags positions that make up more than half of a client's gross exposure.
with positions as (
    select
        client_id,
        symbol,
        sum(signed_quantity) as net_quantity,
        max(executed_at) as last_trade_at
    from {{ ref('fct_trades') }}
    group by client_id, symbol
    having sum(signed_quantity) != 0
),

latest_prices as (
    select symbol, quote as latest_price, quoted_at as price_at
    from {{ ref('stg_ticks') }}
    qualify row_number() over (partition by symbol order by quoted_at desc, tick_id desc) = 1
),

valued as (
    select
        pos.client_id,
        cli.risk_tier,
        pos.symbol,
        pos.net_quantity,
        if(pos.net_quantity > 0, 'LONG', 'SHORT') as direction,
        pos.last_trade_at,
        lp.latest_price,
        lp.price_at,
        pos.net_quantity * sym.contract_size * cast(lp.latest_price as numeric) as market_value
    from positions as pos
    inner join {{ ref('dim_clients') }} as cli using (client_id)
    inner join {{ ref('dim_symbols') }} as sym using (symbol)
    left join latest_prices as lp using (symbol)
)

select
    client_id,
    risk_tier,
    symbol,
    net_quantity,
    direction,
    last_trade_at,
    latest_price,
    price_at,
    market_value,
    abs(market_value) as gross_exposure,
    safe_divide(abs(market_value), sum(abs(market_value)) over (partition by client_id))
        as share_of_client_exposure,
    coalesce(
        safe_divide(abs(market_value), sum(abs(market_value)) over (partition by client_id)) > 0.5,
        false
    ) as is_concentrated,
    current_timestamp() as as_of
from valued
