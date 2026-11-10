-- A price belongs to a row with trades. A row without trades carrying a price would be read as a market price.
select price_key from {{ ref('cut_prices_daily') }}
where not has_trades and weighted_average_usd_cwt is not null
