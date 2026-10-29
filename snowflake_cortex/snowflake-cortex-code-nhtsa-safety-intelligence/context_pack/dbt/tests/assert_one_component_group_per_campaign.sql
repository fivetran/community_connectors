-- Every campaign has one component group, so a make's campaign counts per component group add up to its campaign
-- total. The agent's instructions and agent/AGENTS.md state this; the test fails, one row per campaign, the day a
-- campaign arrives with two groups and the statement stops being true. A null group counts as a group of its own:
-- count(distinct) skips nulls, so a campaign with rows in one group and rows with no group would otherwise pass.

select
  nhtsa_campaign_number,
  count(distinct coalesce(component_group, '<no component group>')) as component_groups
from {{ ref('nhtsa_recall') }}
group by nhtsa_campaign_number
having count(distinct coalesce(component_group, '<no component group>')) > 1
