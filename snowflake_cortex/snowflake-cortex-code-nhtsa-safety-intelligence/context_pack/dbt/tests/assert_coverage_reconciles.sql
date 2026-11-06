-- Coverage reconciles four ways. Per configured vehicle, the campaigns counted from recalls equal the connector's own
-- campaigns_returned. In total, they add up to the recall rows (each row is one campaign x vehicle). The distinct
-- vehicles in the recall rows, which the affected_vehicles metric counts, are the vehicles with has_campaigns. And the
-- model coverage holds every configured model year once. Returns a row (a failure) for each check that does not hold.
-- sum() over no rows is null, not 0, so each sum is coalesced: a sync that loaded nothing reconciles too.

with checks as (
  select
    'vehicle campaign_count <> campaigns_returned' as check_name,
    count(*) as failures
  from {{ ref('nhtsa_coverage') }}
  where campaign_count <> campaigns_returned

  union all

  select
    'sum(campaign_count) <> recall rows' as check_name,
    case
      when (select coalesce(sum(campaign_count), 0) from {{ ref('nhtsa_coverage') }})
        = (select count(*) from {{ ref('nhtsa_recall') }}) then 0
      else 1
    end as failures

  union all

  select
    'distinct recall vehicles <> vehicles with campaigns' as check_name,
    case
      when (
        select count(distinct make || '|' || model || '|' || cast(model_year as varchar))
        from {{ ref('nhtsa_recall') }}
      ) = (select count(*) from {{ ref('nhtsa_coverage') }} where has_campaigns) then 0
      else 1
    end as failures

  union all

  select
    'sum(configured_model_years) <> configured vehicles' as check_name,
    case
      when (select coalesce(sum(configured_model_years), 0) from {{ ref('nhtsa_model_coverage') }})
        = (select count(*) from {{ ref('nhtsa_coverage') }}) then 0
      else 1
    end as failures
)

select *
from checks
where failures > 0
