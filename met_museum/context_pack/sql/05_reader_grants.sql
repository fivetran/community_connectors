-- 05_reader_grants.sql: what a role that owns none of the pack's objects needs to read the pack, kept across every
-- Agents Schema publish and every dbt build.
--
-- Placeholders: <pack_database> (the database that holds the pack schema and AGENTS), <pack_schema> (the schema the
-- dbt models build into), <warehouse> and <reader_role>. Replace them before running the file.
--
-- What each grant is for:
--   the dbt views      USAGE on the database and the pack schema, and SELECT on its views, for an agent with a SQL
--                      tool. dbt replaces a view on every build, so the future grant applies to each view as dbt
--                      recreates it; the grant on all views covers the ones that exist now.
--   the AGENTS schema  USAGE on the schema and SELECT on its tables, for an agent that reads the published context with
--                      SQL. agents-schema 0.0.11 recreates DBT_MODEL, DBT_COLUMN, DBT_DEPENDENCY, the OSI_* tables and
--                      SKILL_USE with CREATE OR REPLACE TABLE on every publish, without COPY GRANTS, so a grant on one
--                      of those tables is gone after the next publish. The future grant applies to each table as the
--                      CLI recreates it.
--   the warehouse      USAGE, so the reader's queries can run.
--
-- The future grants are schema-level. Once a schema has a future grant on an object type, Snowflake ignores every
-- database-level future grant on that type for that schema, for every role. If other roles rely on a database-level
-- future grant in the pack database, repeat it on these two schemas.
--
-- Run it once after the first dbt build and the first publish. Every statement can run twice. Skip the file when only
-- the role that owns the pack's objects reads them.
--
-- Needs: MANAGE GRANTS, or ownership of each object granted on.
grant usage on database <pack_database> to role <reader_role>;
grant usage on warehouse <warehouse> to role <reader_role>;

grant usage on schema <pack_database>.<pack_schema> to role <reader_role>;
grant select on all views in schema <pack_database>.<pack_schema> to role <reader_role>;
grant select on future views in schema <pack_database>.<pack_schema> to role <reader_role>;

grant usage on schema <pack_database>.AGENTS to role <reader_role>;
grant select on all tables in schema <pack_database>.AGENTS to role <reader_role>;
grant select on future tables in schema <pack_database>.AGENTS to role <reader_role>;
