select
    symbol,
    display_name,
    market,
    submarket,
    contract_size,
    quote_currency
from {{ ref('symbols') }}
