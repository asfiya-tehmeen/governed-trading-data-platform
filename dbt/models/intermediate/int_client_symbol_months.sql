-- Position roll-forward per client, symbol and calendar month.
-- Every month from a client's first trade in a symbol onwards gets a row, even
-- months with no trades, so a held position still appears on the statement.
with trades as (
    select * from {{ ref('stg_trades') }}
),

months as (
    select statement_month
    from unnest(generate_date_array(
        (select date_trunc(min(trade_date), month) from trades),
        date_trunc(current_date(), month),
        interval 1 month
    )) as statement_month
),

pairs as (
    select client_id, symbol, date_trunc(min(trade_date), month) as first_month
    from trades
    group by client_id, symbol
),

activity as (
    select
        client_id,
        symbol,
        date_trunc(trade_date, month) as statement_month,
        count(*) as trade_count,
        sum(if(side = 'BUY', quantity, 0)) as bought_quantity,
        sum(if(side = 'SELL', quantity, 0)) as sold_quantity,
        sum(if(side = 'BUY', notional, 0)) as buy_notional,
        sum(if(side = 'SELL', notional, 0)) as sell_notional,
        sum(commission) as commission
    from trades
    group by client_id, symbol, statement_month
),

grid as (
    select p.client_id, p.symbol, m.statement_month
    from pairs as p
    inner join months as m on m.statement_month >= p.first_month
),

filled as (
    select
        g.client_id,
        g.symbol,
        g.statement_month,
        coalesce(a.trade_count, 0) as trade_count,
        coalesce(a.bought_quantity, 0) as bought_quantity,
        coalesce(a.sold_quantity, 0) as sold_quantity,
        coalesce(a.buy_notional, 0) as buy_notional,
        coalesce(a.sell_notional, 0) as sell_notional,
        coalesce(a.commission, 0) as commission
    from grid as g
    left join activity as a using (client_id, symbol, statement_month)
),

rolled as (
    select
        *,
        sum(bought_quantity - sold_quantity) over (
            partition by client_id, symbol
            order by statement_month
            rows between unbounded preceding and current row
        ) as closing_quantity
    from filled
)

select
    client_id,
    symbol,
    statement_month,
    last_day(statement_month, month) as month_end_date,
    statement_month < date_trunc(current_date(), month) as is_month_closed,
    closing_quantity - (bought_quantity - sold_quantity) as opening_quantity,
    trade_count,
    bought_quantity,
    sold_quantity,
    closing_quantity,
    buy_notional,
    sell_notional,
    commission
from rolled
