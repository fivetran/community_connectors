-- A price belongs to a row with trades. A row without trades carrying a price would be read as a market price.
select price_key from {{ ref('stg_usda__cut_prices') }}
where not has_trades
  and (weighted_average_usd_cwt is not null or price_low_usd_cwt is not null or price_high_usd_cwt is not null)
union all
select price_key from {{ ref('stg_usda__grind_prices') }}
where not has_trades
  and (weighted_average_usd_cwt is not null or price_low_usd_cwt is not null or price_high_usd_cwt is not null)
