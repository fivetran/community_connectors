-- Every Met department, live rows only, named in lowercase on every target.

select
  cast({{ lake_col('department_id') }} as integer) as department_id,
  cast({{ lake_col('display_name') }} as varchar) as department_name
from {{ source('met', 'department') }}
where {{ live_rows('department') }}
