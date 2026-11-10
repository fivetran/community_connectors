-- USDA sends ".00" for a cut or item with no trades that day. The connector loads those as null, so a price of zero
-- means a no-trade row leaked through as a real price. It runs on the staging views, so a failure keeps the marts at
-- their last passing build.
select price_key, 'cut' as kind from {{ ref('stg_usda__cut_prices') }}
where weighted_average_usd_cwt = 0 or price_low_usd_cwt = 0 or price_high_usd_cwt = 0
union all
select price_key, 'grind' as kind from {{ ref('stg_usda__grind_prices') }}
where weighted_average_usd_cwt = 0
