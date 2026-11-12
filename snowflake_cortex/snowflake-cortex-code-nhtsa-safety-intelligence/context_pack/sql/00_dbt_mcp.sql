-- 00_dbt_mcp.sql: connect Snowflake to the remote dbt MCP server, so the agent can query the dbt Semantic Layer.
--
-- Placeholders, filled by render_sql.py: {{api_integration}}, {{mcp_server}}, {{dbt_host}}.
--
-- Snowflake supports only OAuth for external MCP servers, so there is no token here: each Snowflake user who runs the
-- agent connects once (Snowflake Intelligence > Settings > MCP Connectors > the server > Connect), and that consent
-- carries over to agent runs from SQL. dbt needs the remote MCP OAuth connection (Starter or Enterprise) and a static
-- subdomain; {{dbt_host}} is that host, for example abc123.us1.dbt.com.
--
-- "if not exists" keeps an existing integration and server, and with them every user's consent. Dropping and
-- recreating either one means each user consents again.
--
-- Needs: CREATE INTEGRATION on the account, CREATE EXTERNAL MCP SERVER on the pack schema.

create api integration if not exists {{api_integration}}
  api_provider = external_mcp
  api_allowed_prefixes = ('https://{{dbt_host}}/api/ai/v1/mcp')
  api_user_authentication = (
    type = oauth_dynamic_client
    oauth_client_auth_method = none
    oauth_token_endpoint = 'https://{{dbt_host}}/oauth/token'
    oauth_authorization_endpoint = 'https://{{dbt_host}}/oauth/authorize'
    oauth_resource_url = 'https://{{dbt_host}}/api/ai/v1/mcp'
    oauth_allowed_scopes = ('user_access', 'offline_access')
  )
  enabled = true
  comment = 'NHTSA Recalls context pack: remote dbt MCP server';

create external mcp server if not exists {{mcp_server}}
  with display_name = 'dbt Semantic Layer MCP (NHTSA Recalls)'
  url = 'https://{{dbt_host}}/api/ai/v1/mcp'
  api_integration = {{api_integration}};
