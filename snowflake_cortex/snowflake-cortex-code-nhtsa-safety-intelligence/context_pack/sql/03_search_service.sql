-- 03_search_service.sql: Cortex Search over the recall campaign text, one document per campaign.
--
-- Placeholders, filled by render_sql.py: {{pack}}, {{warehouse}}, {{target_lag}}.
--
-- Run after the dbt Prod job has built nhtsa_campaign_search into {{pack}}. The service answers the questions counts
-- cannot, such as "which campaigns involve a fire risk"; every count goes through the dbt Semantic Layer instead.
--
-- makes and models are sorted arrays, so a search filters on one make with
-- { "@contains": { "makes": "Ford" } } and a campaign that covers two makes stays one document.
--
-- Incremental refresh needs change tracking on every object under the view. A Fivetran Managed Data Lake table read
-- through a catalog-linked database has it on; on a standard Fivetran Snowflake destination, run
-- alter table <recalls table> set change_tracking = true. The view is deterministic (no any_value) for the same reason.
--
-- Cost: {{warehouse}} runs each refresh, at most once per {{target_lag}}, plus a serving charge while the service
-- exists. 99_teardown.sql drops it.
--
-- Needs: CREATE CORTEX SEARCH SERVICE on the pack schema, USAGE on the warehouse, SELECT on nhtsa_campaign_search.

create or replace cortex search service {{pack}}.nhtsa_campaign_search_service
  on search_text
  attributes makes, models, component_group, model_year_min, model_year_max, report_received_date
  warehouse = {{warehouse}}
  target_lag = '{{target_lag}}'
  comment = 'NHTSA Recalls context pack: campaign text, one document per campaign'
  as
    select
      nhtsa_campaign_number,
      search_text,
      makes,
      models,
      manufacturer,
      component_group,
      model_year_min,
      model_year_max,
      report_received_date,
      park_it,
      park_outside,
      over_the_air_update,
      summary,
      consequence,
      remedy
    from {{pack}}.nhtsa_campaign_search;
