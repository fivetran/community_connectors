-- price_per_lb is the USDA price per 100 pounds divided by 100, in every price table, and is null exactly when
-- the price per 100 pounds is null. It runs on the marts, so it guards their SQL, not the synced data.
select price_key from {{ ref('cut_prices_daily') }}
where (price_per_lb is null) <> (weighted_average_usd_cwt is null)
   or (price_low_per_lb is null) <> (price_low_usd_cwt is null)
   or (price_high_per_lb is null) <> (price_high_usd_cwt is null)
   or abs(price_per_lb * 100 - weighted_average_usd_cwt) > 1e-9
   or abs(price_low_per_lb * 100 - price_low_usd_cwt) > 1e-9
   or abs(price_high_per_lb * 100 - price_high_usd_cwt) > 1e-9
union all
select price_key from {{ ref('grind_prices_daily') }}
where (price_per_lb is null) <> (weighted_average_usd_cwt is null)
   or abs(price_per_lb * 100 - weighted_average_usd_cwt) > 1e-9
