-- Last tick of each UTC day per symbol, used to value positions.
select
    symbol,
    quote_date as price_date,
    quote as close_price,
    quoted_at as close_quoted_at
from {{ ref('stg_ticks') }}
qualify row_number() over (partition by symbol, quote_date order by quoted_at desc, tick_id desc) = 1
