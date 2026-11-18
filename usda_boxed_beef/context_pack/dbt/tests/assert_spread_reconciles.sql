-- The spread is the Choice cutout minus the Select cutout, null exactly when either cutout is null, and both cutouts
-- match daily_cutout.
-- It runs on the marts, so it guards their SQL, not the synced data.
select s.report_date
from {{ ref('choice_select_spread') }} as s
left join {{ ref('daily_cutout') }} as c
  on c.report_date = s.report_date and c.grade = 'Choice'
left join {{ ref('daily_cutout') }} as x
  on x.report_date = s.report_date and x.grade = 'Select'
where (s.spread_usd_cwt is null) <> ((s.choice_cutout_usd_cwt - s.select_cutout_usd_cwt) is null)
   or abs(s.spread_usd_cwt - (s.choice_cutout_usd_cwt - s.select_cutout_usd_cwt)) > 1e-9
   or c.cutout_usd_cwt is distinct from s.choice_cutout_usd_cwt
   or x.cutout_usd_cwt is distinct from s.select_cutout_usd_cwt
