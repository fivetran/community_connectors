-- The connector keeps one row per report and report date; a correction replaces it in place.
select slug_id, report_date, count(*) as n
from {{ ref('stg_usda__reports') }}
group by slug_id, report_date
having count(*) > 1
