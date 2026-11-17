-- Every model an agent reads must carry a description: it is the context the Semantic Layer and the MCP server
-- answer with. The pack builds every model into one schema, so every model in the project is checked.
select unique_id, name
from {{ info_schema('models') }}
where coalesce(trim(description), '') = ''
