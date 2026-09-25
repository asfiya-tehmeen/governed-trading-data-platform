-- Month-end client statement: one row per client, month and symbol held or traded.
-- Positions are valued at the last available close on or before month end.
-- For the current (open) month, is_month_closed is false and the valuation is provisional.
with positions as (
    select * from {{ ref('int_client_symbol_months') }}
),

symbol_month_prices as (
    select
        m.symbol,
        m.statement_month,
        p.close_price as month_end_price,
        p.price_date as month_end_price_date
    from (select distinct symbol, statement_month, month_end_date from positions) as m
    left join {{ ref('int_daily_close_prices') }} as p
        on p.symbol = m.symbol and p.price_date <= m.month_end_date
    qualify row_number() over (
        partition by m.symbol, m.statement_month order by p.price_date desc
    ) = 1
)

select
    pos.client_id,
    pos.statement_month,
    pos.is_month_closed,
    pos.symbol,
    sym.quote_currency,
    pos.opening_quantity,
    pos.trade_count,
    pos.bought_quantity,
    pos.sold_quantity,
    pos.closing_quantity,
    pos.buy_notional,
    pos.sell_notional,
    pos.commission,
    prc.month_end_price,
    prc.month_end_price_date,
    pos.closing_quantity * sym.contract_size * cast(prc.month_end_price as numeric)
        as closing_market_value,
    current_timestamp() as generated_at
from positions as pos
inner join {{ ref('dim_symbols') }} as sym using (symbol)
left join symbol_month_prices as prc using (symbol, statement_month)
