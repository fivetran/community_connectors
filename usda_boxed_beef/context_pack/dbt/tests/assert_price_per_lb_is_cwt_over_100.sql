-- price_per_lb is the USDA price per 100 pounds divided by 100, in every price view.
select price_key from {{ ref('cut_prices_daily') }}
where abs(price_per_lb * 100 - weighted_average_usd_cwt) > 1e-9
   or abs(price_low_per_lb * 100 - price_low_usd_cwt) > 1e-9
   or abs(price_high_per_lb * 100 - price_high_usd_cwt) > 1e-9
union all
select price_key from {{ ref('grind_prices_daily') }}
where abs(price_per_lb * 100 - weighted_average_usd_cwt) > 1e-9
