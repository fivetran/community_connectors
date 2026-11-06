-- A source table nobody reads is either dead weight or a model that never got written.
select s.unique_id, s.name
from {{ info_schema('sources') }} as s
left join {{ info_schema('edges') }} as e on e.parent_unique_id = s.unique_id
where e.child_unique_id is null
