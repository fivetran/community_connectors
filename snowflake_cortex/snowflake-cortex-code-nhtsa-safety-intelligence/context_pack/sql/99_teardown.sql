-- 99_teardown.sql: drop everything 00_dbt_mcp.sql to 04_agent.sql created, and the views the dbt Prod job built.
-- The Agents Schema tables in <pack database>.AGENTS stay: other agents may read them, and the next publish
-- replaces them. Drop that schema yourself if nothing else uses it.
--
-- Placeholders, filled by render_sql.py: {{pack}}, {{mcp_server}}.
--
-- Order matters: the agent uses the search service, the agents_context procedure and the MCP server, the search service reads the
-- nhtsa_campaign_search view, and the MCP server uses the integration. Every drop is "if exists", so a second run is
-- harmless. The pack database and schema stay; drop them yourself if nothing else lives there.
--
-- Dropping the integration or the MCP server removes every user's dbt consent; recreating them needs a new consent.
-- The API integration from 00_dbt_mcp.sql stays. It is account-level and other external MCP servers can use it, and
-- dropping it would stop them working. When show external mcp servers lists no other server on it, drop it yourself
-- after this file runs (comment lines are not rendered, so put in your name):
-- drop api integration if exists <api_integration>;
-- The dbt platform project is not touched here: remove it in dbt platform.
--
-- If 05_reader_grants.sql ran, run 98_reader_revoke.sql first. The drops below take the grants on the agent, the
-- search service, the procedure and the MCP server with them, but not the reader's grants on the database, the pack
-- schema, the warehouse, the integration, the AGENTS schema and its future tables, or the Cortex database role.

drop agent if exists {{pack}}.nhtsa_recalls_agent;

drop cortex search service if exists {{pack}}.nhtsa_campaign_search_service;
drop procedure if exists {{pack}}.agents_context(varchar);

drop external mcp server if exists {{mcp_server}};

-- The dbt models. dbt recreates them on the next Prod run.
drop view if exists {{pack}}.nhtsa_campaign_search;
drop view if exists {{pack}}.nhtsa_sync_window;
drop view if exists {{pack}}.nhtsa_model_coverage;
drop view if exists {{pack}}.nhtsa_coverage;
drop view if exists {{pack}}.nhtsa_component;
drop view if exists {{pack}}.nhtsa_campaign;
drop view if exists {{pack}}.nhtsa_recall;
drop view if exists {{pack}}.stg_nhtsa__configured_vehicle;
drop view if exists {{pack}}.stg_nhtsa__recalls;
drop view if exists {{pack}}.metricflow_time_spine;
