-- USDA sends ".00" for a cut or item with no trades that day. The connector loads those as null, so a price of zero
-- in any view means a no-trade row leaked through as a real price.
select price_key, 'cut' as kind from {{ ref('cut_prices_daily') }}
where weighted_average_usd_cwt = 0 or price_low_usd_cwt = 0 or price_high_usd_cwt = 0
union all
select price_key, 'grind' as kind from {{ ref('grind_prices_daily') }}
where weighted_average_usd_cwt = 0
