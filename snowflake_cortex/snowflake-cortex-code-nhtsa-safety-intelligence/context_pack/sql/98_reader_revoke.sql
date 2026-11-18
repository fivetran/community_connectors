-- 98_reader_revoke.sql: remove every grant 05_reader_grants.sql made to the reader role.
--
-- Placeholders, filled by render_sql.py: {{database}}, {{pack}}, {{agents}}, {{warehouse}}, {{mcp_server}},
-- {{api_integration}}, {{reader_role}}.
--
-- Run it before 99_teardown.sql. The teardown drops the agent, the search service, the procedure and the MCP server,
-- and their grants go with them, but the grants on the database, the pack schema, the warehouse, the integration,
-- the AGENTS schema and its tables, the future grant and the Cortex database role stay until revoked. Revoking a grant
-- the role does not hold is not an error, so the file can run twice. Run the revokes on objects the teardown drops
-- before the teardown: after it, those statements fail because the object no longer exists.
--
-- Skip the grants the reader role needs for other work: usage on the database or the warehouse, or the Cortex
-- database role, may have been granted for something besides the pack.
--
-- Needs: the same privileges as 05_reader_grants.sql.
revoke select on future tables in schema {{agents}} from role {{reader_role}};
revoke select on all tables in schema {{agents}} from role {{reader_role}};
revoke usage on schema {{agents}} from role {{reader_role}};
revoke database role snowflake.cortex_agent_user from role {{reader_role}};
revoke usage on integration {{api_integration}} from role {{reader_role}};
revoke usage on external mcp server {{mcp_server}} from role {{reader_role}};
revoke usage on warehouse {{warehouse}} from role {{reader_role}};
revoke usage on procedure {{pack}}.agents_context(varchar) from role {{reader_role}};
revoke usage on cortex search service {{pack}}.nhtsa_campaign_search_service from role {{reader_role}};
revoke usage on agent {{pack}}.nhtsa_recalls_agent from role {{reader_role}};
revoke usage on schema {{pack}} from role {{reader_role}};
revoke usage on database {{database}} from role {{reader_role}};
