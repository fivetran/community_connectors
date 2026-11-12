-- Every recall row lands in exactly one campaign: the campaigns' vehicle_rows add up to the recall rows.
-- Returns a row (a failure) when they do not. sum() over no rows is null, and a comparison with null is never true,
-- so the sum is coalesced: otherwise recall rows with no campaigns would pass.

with totals as (
  select
    (select count(*) from {{ ref('nhtsa_recall') }}) as recall_rows,
    (select coalesce(sum(vehicle_rows), 0) from {{ ref('nhtsa_campaign') }}) as campaign_vehicle_rows
)

select *
from totals
where recall_rows <> campaign_vehicle_rows
