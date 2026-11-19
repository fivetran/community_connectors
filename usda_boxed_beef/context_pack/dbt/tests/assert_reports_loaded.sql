-- The connector has loaded at least one report. On an empty sync this fails, so the marts are not built empty and an
-- agent never answers "none" from tables that hold no data.
select 'no report loaded' as problem
where not exists (select 1 from {{ ref('stg_usda__reports') }})
