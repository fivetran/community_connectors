-- 05_reader_grants.sql: everything a role that owns none of the pack's objects needs to ask the agent, kept across
-- every Agents Schema publish.
--
-- Placeholders, filled by render_sql.py: {{database}}, {{pack}}, {{agents}}, {{warehouse}}, {{mcp_server}},
-- {{api_integration}}, {{reader_role}}.
--
-- What each grant is for:
--   the agent          USAGE on the agent, and on the database and schema that hold it.
--   campaign_search    USAGE on the Cortex Search service.
--   agents_context     USAGE on the procedure. It runs as caller, so the reader also needs USAGE on the AGENTS schema
--                      and SELECT on its tables, and USAGE on the warehouse the agent runs it on.
--   the dbt MCP tools  USAGE on the external MCP server and on its API integration. Each reader also connects once in
--                      Snowflake Intelligence (00_dbt_mcp.sql); a grant cannot give that consent. The dbt views need no
--                      grant for the agent: the Semantic Layer queries them with the dbt platform's own credential.
--                      A SQL tool that reads the views directly needs SELECT on them; the README gives the grants.
--   Cortex Agents      the SNOWFLAKE.CORTEX_AGENT_USER database role. SNOWFLAKE.CORTEX_USER, which also allows the
--                      agent, is granted to PUBLIC by default; the grant below still works where an administrator
--                      revoked that.
--
-- agents-schema 0.0.11 recreates DBT_MODEL, DBT_COLUMN, DBT_DEPENDENCY, the OSI_* tables and SKILL_USE with CREATE OR
-- REPLACE TABLE on every publish, without COPY GRANTS, so a grant on one of those tables is gone after the next
-- publish. The future grant applies to each table as the CLI recreates it; the grant on all tables covers the ones
-- that exist now.
--
-- The future grant is schema-level. Once a schema has a future grant on tables, Snowflake ignores every
-- database-level future grant on tables for that schema, for every role. If other roles rely on a future grant on
-- tables in the pack database, repeat it on the AGENTS schema (comment lines are not rendered, so put in your names):
-- grant select on future tables in schema <pack_database>.AGENTS to role <that role>. The pack's AGENTS schema holds
-- only the publish's tables, so the grant stays at schema level rather than giving the reader every future table in
-- the database.
--
-- Run it after 02_agents_context.sql, 03_search_service.sql and 04_agent.sql, and again after re-running 02 or 03:
-- CREATE OR REPLACE drops the grants on the procedure and on the search service. 04 replaces the agent with COPY
-- GRANTS, so the agent grant survives a re-run of 04. Every statement can run twice. Skip the file when only the role
-- that owns the pack's objects asks the agent. 98_reader_revoke.sql removes every grant made here.
--
-- Needs: MANAGE GRANTS, or ownership of each object granted on. The integration is account-level and usually owned by
-- an administrator.
grant usage on database {{database}} to role {{reader_role}};
grant usage on schema {{pack}} to role {{reader_role}};
grant usage on agent {{pack}}.nhtsa_recalls_agent to role {{reader_role}};
grant usage on cortex search service {{pack}}.nhtsa_campaign_search_service to role {{reader_role}};
grant usage on procedure {{pack}}.agents_context(varchar) to role {{reader_role}};
grant usage on warehouse {{warehouse}} to role {{reader_role}};
grant usage on external mcp server {{mcp_server}} to role {{reader_role}};
grant usage on integration {{api_integration}} to role {{reader_role}};
grant database role snowflake.cortex_agent_user to role {{reader_role}};
grant usage on schema {{agents}} to role {{reader_role}};
grant select on all tables in schema {{agents}} to role {{reader_role}};
grant select on future tables in schema {{agents}} to role {{reader_role}};
