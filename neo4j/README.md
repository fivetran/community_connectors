# Neo4j Database Connector Example

## Connector overview

This connector shows how to extract data from Neo4j graph databases and upsert it using Fivetran Connector SDK.

## Requirements

- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```
fivetran init --template neo4j
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.


## Features

- Connect to Neo4j graph databases using official Neo4j Python driver.
- Stream data with minimal memory usage through pagination techniques using `skip` and `batch_size` parameters.
- Uses Cypher queries to extract specific data patterns.
- Error handling for connection issues, authentication problems, and query failures.


## Configuration file

The connector requires the following configuration parameters:

```json
{
  "neo4j_uri": "<YOUR_NEO4J_URI>",
  "username": "<YOUR_NEO4J_USERNAME>",
  "password": "<YOUR_NEO4J_PASSWORD>",
  "database": "<YOUR_NEO4J_DATABASE>"
}
```

- `neo4j_uri`: The URI to connect to your Neo4j database (supports bolt, bolt+s, neo4j, neo4j+s protocols)
- `username`: Neo4j database username
- `password`: Neo4j database password
- `database`: Name of the specific Neo4j database to connect to

> Note: When submitting connector code as a [Community Connector](https://github.com/fivetran/community_connectors/tree/main) in the open-source [Connector SDK repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.


## Requirements file

The connector requires the following Python packages:

```
neo4j==6.1.0
neo4j-driver==5.28.3
```

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.


## Authentication

This connector uses basic authentication with Neo4j databases, requiring a username and password. You can also modify the code to allow authentication using the supported authentications methods provided by the Neo4j driver.

For testing the example, you can use the following public credentials provided by neo4j for their demo `twitter` database:

```json
{
  "neo4j_uri": "neo4j+s://demo.neo4jlabs.com:7687",
  "username": "neo4j",
  "password": "password",
  "database": "neo4j"
}
```

## Pagination

The connector implements pagination strategies using the following:
- Skip/Limit pagination: Records are fetched in batches using `SKIP` and `LIMIT` Cypher query parameters.

Pagination batch sizes are configurable, allowing for customization based on your specific requirements.

## Data handling

The connector processes Neo4j data as follows:
- Data extraction: Executes Cypher queries to extract data from Neo4j graph structures
- Type conversion: Converts the datetime format from Neo4j to a standard format compatible with Fivetran

The current implementation supports the following tables:
- users: User information including followers, following, and profile details
- tweet_hashtags: Relationships between tweets and hashtags

## Error handling

The connector implements comprehensive error handling:
- Connection issues: Catches ServiceUnavailable exceptions when Neo4j is unreachable
- Authentication problems: Handles AuthError exceptions for invalid credentials
- Query errors: Captures and logs any issues with Cypher query execution
- Resource management: Uses try/finally blocks to ensure driver connections are properly closed
- Detailed logging: Provides informative log messages to aid in troubleshooting


## Tables created

### `USER`

The schema for the `USER` table is as follows:

```json
{
  "table": "user",
  "primary_key": ["username"],
  "columns": {
    "username": "STRING"
  }
}
```

### `TWEET_HASHTAG`

The schema for the `TWEET_HASHTAG` table is as follows:

```json
{
  "table": "tweet_hashtag",
  "primary_key": ["tweet_id"],
  "columns": {
    "tweet_id": "STRING"
  }
}
```


## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
