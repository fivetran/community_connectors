-- Every recall row lands in exactly one campaign: the campaigns' vehicle_rows add up to the recall rows.
-- Returns a row (a failure) when they do not.

with totals as (
  select
    (select count(*) from {{ ref('nhtsa_recall') }}) as recall_rows,
    (select sum(vehicle_rows) from {{ ref('nhtsa_campaign') }}) as campaign_vehicle_rows
)

select *
from totals
where recall_rows <> campaign_vehicle_rows
