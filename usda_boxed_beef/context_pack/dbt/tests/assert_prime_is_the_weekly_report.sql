-- Prime prices come only from the weekly report 2460, and that report holds only Prime.
select price_key from {{ ref('stg_usda__cut_prices') }}
where (grade = 'Prime') <> (slug_id = '2460')
   or (slug_id = '2460') <> (cadence = 'weekly')
