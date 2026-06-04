<p align="center">
  <a href="https://www.fivetran.com/">
    <img src="https://cdn.prod.website-files.com/6130fa1501794ed4d11867ba/63d9599008ad50523f8ce26a_logo.svg" alt="Fivetran">
  </a>
</p>

<p align="center">
  Fivetran Connector SDK allows Real-time, efficient data replication to your destination of choice.
</p>

<p align="center">
  <a href="https://github.com/fivetran/community_connectors/stargazers" target="_blank"><img src="https://img.shields.io/github/stars/fivetran/community_connectors?style=social&label=Star"></a>
  <a href="https://github.com/fivetran/community_connectors/blob/main/LICENSE" target="_blank"><img src="https://img.shields.io/badge/License-MIT-blue" alt="License"></a>
  <a href="https://github.com/fivetran/community_connectors/blob/main/README.md" target="_blank"><img src="https://img.shields.io/badge/Managed-Yes-green" alt="Managed"></a>
</p>

# Overview

This repository contains 100+ connectors built with the [Fivetran Connector SDK](https://fivetran.com/docs/connectors/connector-sdk). Each connector is designed to work with minimal modification, so you can quickly adapt it to your source. Browse by category or search for your specific data source.

For SDK installation and setup, visit the main [Fivetran Connector SDK repository](https://github.com/fivetran/fivetran_connector_sdk).

<details class="details-heading" open="open">
<summary>📋 Full List of Connectors</summary>

### Databases

- **[apache_druid/using_pydruid](https://github.com/fivetran/community_connectors/tree/main/apache_druid/using_pydruid)** - This example demonstrates syncing from Apache Druid using the PyDruid library for native query capabilities, retry logic with exponential backoff, and optimized data retrieval for enhanced performance.
- **[apache_druid/using_sql](https://github.com/fivetran/community_connectors/tree/main/apache_druid/using_sql)** - This example shows how to sync data from Apache Druid datasources using Druid's SQL API with time-based pagination and incremental sync to efficiently replicate event and analytics data.
- **[apache_hbase](https://github.com/fivetran/community_connectors/tree/main/apache_hbase)** - Connect and sync data from Apache HBase using happybase and thrift libraries
- **[apache_hive/using_pyhive](https://github.com/fivetran/community_connectors/tree/main/apache_hive/using_pyhive)** - Sync data from Apache Hive using PyHive
- **[apache_hive/using_sqlalchemy](https://github.com/fivetran/community_connectors/tree/main/apache_hive/using_sqlalchemy)** - Sync data from Apache Hive using SQLAlchemy with PyHive
- **[arango_db](https://github.com/fivetran/community_connectors/tree/main/arango_db)** - Sync document and edge collections from ArangoDB multi-model database
- **[cassandra](https://github.com/fivetran/community_connectors/tree/main/cassandra)** - Connect and sync data from Cassandra database
- **[clickhouse](https://github.com/fivetran/community_connectors/tree/main/clickhouse)** - Sync data from ClickHouse database
- **[couchbase_capella](https://github.com/fivetran/community_connectors/tree/main/couchbase_capella)** - Sync data from Couchbase Capella
- **[couchbase_magma](https://github.com/fivetran/community_connectors/tree/main/couchbase_magma)** - Sync data from self-managed Couchbase Server using Magma storage
- **[dgraph](https://github.com/fivetran/community_connectors/tree/main/dgraph)** - Sync e-commerce product catalog from Dgraph graph databases
- **[documentdb](https://github.com/fivetran/community_connectors/tree/main/documentdb)** - Connect to AWS DocumentDB and sync collections (Hybrid Deployment compatible)
- **[dolphin_db](https://github.com/fivetran/community_connectors/tree/main/dolphin_db)** - Sync data from DolphinDB database
- **[dragonfly_db](https://github.com/fivetran/community_connectors/tree/main/dragonfly_db)** - Sync high-performance in-memory data from DragonflyDB
- **[ehi/simple_ehi](https://github.com/fivetran/community_connectors/tree/main/ehi/simple_ehi)** - This example shows how to sync EHI tables such as Caboodle using Connector SDK. It connects to Microsoft SQL server to sync the data. You need to provide your server credentials for this example to work.
- **[ehi/high_volume_ehi](https://github.com/fivetran/community_connectors/tree/main/ehi/high_volume_ehi)** - High-volume Microsoft SQL Server connector for syncing high volume EHI tables using keyset and offset pagination with parallel processing
- **[firebird_db](https://github.com/fivetran/community_connectors/tree/main/firebird_db)** - Sync data from Firebird DB
- **[greenplum_db](https://github.com/fivetran/community_connectors/tree/main/greenplum_db)** - Sync data from Greenplum database
- **[ibm_db2](https://github.com/fivetran/community_connectors/tree/main/ibm/ibm_db2)** - This example shows how to connect and sync data from IBM Db2 using Connector SDK. It uses the `ibm_db` library to connect to the database and fetch data.
- **[ibm_db2_log_based_replication](https://github.com/fivetran/community_connectors/tree/main/ibm/ibm_db2_log_based_replication)** - This example demonstrates log-based Change Data Capture (CDC) for IBM Db2 using the ASN SQL Replication framework. The `asncap` daemon reads the Db2 transaction log and writes every INSERT, UPDATE, and DELETE to a Change Data table; the connector reads exclusively from that table after the initial load, making this genuine log-based replication without polling the source.
- **[ibm_informix_using_ibm_db](https://github.com/fivetran/community_connectors/tree/main/ibm/ibm_informix_using_ibm_db)** - This example shows how to connect and sync data from IBM Informix using Connector SDK. This example uses the `ibm_db` library to connect to the Informix database and fetch data.
- **[ibm_informix_using_jaydebeapi](https://github.com/fivetran/community_connectors/tree/main/ibm/ibm_informix_using_jaydebeapi)** - This example shows how to connect and sync data from IBM Informix using Connector SDK. This example uses the `jaydebeapi` library with external JDBC Informix driver, using `installation.sh` file in the `drivers` folder, to connect to the Informix database and fetch data.
- **[janus_graph](https://github.com/fivetran/community_connectors/tree/main/janus_graph)** - This example shows how to sync data from the Janus Graph database using the Connector SDK. You need to provide your Janus Graph credentials for this example to work.
- **[influx_db](https://github.com/fivetran/community_connectors/tree/main/influx_db)** - Sync time-series data from InfluxDB
- **[neo4j](https://github.com/fivetran/community_connectors/tree/main/neo4j)** - Extract data from Neo4j graph databases
- **[quest_db](https://github.com/fivetran/community_connectors/tree/main/quest_db)** - Sync high-performance time series data from QuestDB
- **[raven_db](https://github.com/fivetran/community_connectors/tree/main/raven_db)** - Sync document data from RavenDB NoSQL database
- **[redis](https://github.com/fivetran/community_connectors/tree/main/redis)** - Sync gaming leaderboards and player statistics from Redis
- **[rethink_db](https://github.com/fivetran/community_connectors/tree/main/rethink_db)** - Sync data from RethinkDB real-time database
- **[sap_hana_sql](https://github.com/fivetran/community_connectors/tree/main/sap_hana_sql)** - Connect to SAP HANA SQL Server using hdbcli
- **[sql_server](https://github.com/fivetran/community_connectors/tree/main/sql_server)** - Connect to SQL Server using pyodbc
- **[sybase_iq](https://github.com/fivetran/community_connectors/tree/main/sybase_iq)** - This example demonstrates how to connect to Sybase IQ database using the `FreeTDS` driver and `PyODBC` by writing a `installation.sh` file in the `drivers` folder.
- **[sybase_ase](https://github.com/fivetran/community_connectors/tree/main/sybase_ase)** - This example demonstrates how to connect to Sybase ASE database using the `FreeTDS` driver and `PyODBC` by writing a `installation.sh` file in the `drivers` folder.
- **[teradata](https://github.com/fivetran/community_connectors/tree/main/teradata)** - Sync data from Teradata Vantage database
- **[tidb](https://github.com/fivetran/community_connectors/tree/main/tidb)** - Incremental replication from TiDB databases
- **[timescale_db](https://github.com/fivetran/community_connectors/tree/main/timescale_db)** - Sync time-series and vector data from TimescaleDB
- **[yugabyte_db](https://github.com/fivetran/community_connectors/tree/main/yugabyte_db)** - Sync data from YugabyteDB distributed SQL database

### Cloud Data Warehouses

- **[aws_athena/using_boto3](https://github.com/fivetran/community_connectors/tree/main/aws_athena/using_boto3)** - Sync data from AWS Athena using Boto3
- **[aws_athena/using_sqlalchemy](https://github.com/fivetran/community_connectors/tree/main/aws_athena/using_sqlalchemy)** - Sync data from AWS Athena using SQLAlchemy with PyAthena
- **[aws_dynamo_db_authentication](https://github.com/fivetran/community_connectors/tree/main/aws_dynamo_db_authentication)** - Connect and sync data from AWS DynamoDB
- **[aws_rds_oracle](https://github.com/fivetran/community_connectors/tree/main/aws_rds_oracle)** - Connect and sync data from AWS Oracle
- **[redshift/simple_redshift_connector](https://github.com/fivetran/community_connectors/tree/main/redshift/simple_redshift_connector)** - Sync records from Redshift
- **[redshift/large_data_volume](https://github.com/fivetran/community_connectors/tree/main/redshift/large_data_volume)** - Sync large data volumes from Redshift
- **[redshift/using_unload](https://github.com/fivetran/community_connectors/tree/main/redshift/using_unload)** - Sync data from Redshift using UNLOAD to S3

### Message Queues & Streaming

- **[apache_pulsar](https://github.com/fivetran/community_connectors/tree/main/apache_pulsar)** - Fetch data from Apache Pulsar topics with Reader API
- **[gcp_pub_sub](https://github.com/fivetran/community_connectors/tree/main/gcp_pub_sub)** - Sync data from Google Cloud Pub/Sub
- **[rabbitmq](https://github.com/fivetran/community_connectors/tree/main/rabbitmq)** - Sync messages from RabbitMQ queues
- **[solace](https://github.com/fivetran/community_connectors/tree/main/solace)** - Sync messages from Solace queue

### SaaS & APIs

- **[amazon_video_central](https://github.com/fivetran/community_connectors/tree/main/amazon_video_central)** - Sync report data from Amazon Video Central API
- **[awardco](https://github.com/fivetran/community_connectors/tree/main/awardco)** - Sync data from Awardco rewards platform
- **[betterstack](https://github.com/fivetran/community_connectors/tree/main/betterstack)** - Sync uptime monitoring data from Better Stack
- **[callminer](https://github.com/fivetran/fivetran_csdk_connectors/tree/main/callminer)** - Sync CallMiner Bulk Export data using OAuth2 authentication, export job polling, archive extraction, and per-data-type incremental state tracking
- **[checkly](https://github.com/fivetran/community_connectors/tree/main/checkly)** - Sync monitoring check data and analytics from Checkly
- **[clerk](https://github.com/fivetran/community_connectors/tree/main/clerk)** - Sync user data from Clerk authentication
- **[commonpaper](https://github.com/fivetran/community_connectors/tree/main/commonpaper)** - Sync agreement data from Common Paper
- **[courier](https://github.com/fivetran/community_connectors/tree/main/courier)** - Sync notifications data from Courier multi-channel platform
- **[customer_thermometer](https://github.com/fivetran/community_connectors/tree/main/customer_thermometer)** - Sync customer feedback from Customer Thermometer
- **[data_camp](https://github.com/fivetran/community_connectors/tree/main/data_camp)** - Sync course catalog from DataCamp LMS
- **[discord](https://github.com/fivetran/community_connectors/tree/main/discord)** - Sync data from Discord
- **[docusign](https://github.com/fivetran/community_connectors/tree/main/docusign)** - Sync data from Docusign eSignature API
- **[elastic_email](https://github.com/fivetran/community_connectors/tree/main/elastic_email)** - Sync email marketing data from Elastic Email
- **[fleetio](https://github.com/fivetran/community_connectors/tree/main/fleetio)** - Sync fleet management data from Fleetio
- **[fred](https://github.com/fivetran/community_connectors/tree/main/fred)** - Sync economic data from Federal Reserve Economic Data (FRED)
- **[github](https://github.com/fivetran/community_connectors/tree/main/github)** - Sync repository data, commits, and pull requests from GitHub
- **[github_traffic](https://github.com/fivetran/community_connectors/tree/main/github_traffic)** - Sync GitHub repository traffic data
- **[gnews](https://github.com/fivetran/community_connectors/tree/main/gnews)** - Sync news articles from GNews API
- **[google_trends](https://github.com/fivetran/community_connectors/tree/main/google_trends)** - Sync search interest data from Google Trends
- **[goshippo](https://github.com/fivetran/community_connectors/tree/main/goshippo)** - Sync shipment data from Goshippo API
- **[grey_hr](https://github.com/fivetran/community_connectors/tree/main/grey_hr)** - Sync HR data from greytHR API
- **[gumroad](https://github.com/fivetran/community_connectors/tree/main/gumroad)** - Sync sales, products, and subscribers from Gumroad
- **[harness_io](https://github.com/fivetran/community_connectors/tree/main/harness_io)** - Connect and sync data from Harness.io
- **[healthchecks](https://github.com/fivetran/community_connectors/tree/main/healthchecks)** - Sync health check monitoring from Healthchecks.io
- **[hubspot](https://github.com/fivetran/community_connectors/tree/main/hubspot)** - Sync event data from HubSpot
- **[iterate](https://github.com/fivetran/community_connectors/tree/main/iterate)** - Sync NPS survey data from Iterate REST API
- **[keycloak](https://github.com/fivetran/community_connectors/tree/main/keycloak)** - Sync IAM data from Keycloak Admin API
- **[leavedates](https://github.com/fivetran/community_connectors/tree/main/leavedates)** - Sync leave report data from LeaveDates API
- **[mailerlite](https://github.com/fivetran/community_connectors/tree/main/mailerlite)** - Sync email marketing data from MailerLite
- **[mastertax](https://github.com/fivetran/community_connectors/tree/main/mastertax)** - Sync data from MasterTax API
- **[meilisearch](https://github.com/fivetran/community_connectors/tree/main/meilisearch)** - Sync index metadata and documents from MeiliSearch
- **[microsoft_excel](https://github.com/fivetran/community_connectors/tree/main/microsoft_excel)** - Sync data from Microsoft Excel files
- **[microsoft_intune](https://github.com/fivetran/community_connectors/tree/main/microsoft_intune)** - Retrieve managed devices from Microsoft Intune
- **[n8n](https://github.com/fivetran/community_connectors/tree/main/n8n)** - Sync workflow automation data from n8n
- **[netlify](https://github.com/fivetran/community_connectors/tree/main/netlify)** - Sync sites, deploys, and forms from Netlify API
- **[newsapi](https://github.com/fivetran/community_connectors/tree/main/newsapi)** - Sync news articles from NewsAPI
- **[noaa](https://github.com/fivetran/community_connectors/tree/main/noaa)** - Sync weather observations from National Weather Service
- **[npi_registry](https://github.com/fivetran/community_connectors/tree/main/npi_registry)** - Sync healthcare provider data from NPPES NPI Registry
- **[oauth2_and_accelo_api_connector_multithreading_enabled](https://github.com/fivetran/community_connectors/tree/main/oauth2_and_accelo_api_connector_multithreading_enabled)** - Sync data from Accelo API with OAuth 2.0 and multithreading
- **[odata_api](https://github.com/fivetran/community_connectors/tree/main/odata_api)** - Sync data from OData APIs (versions 2 and 4)
- **[oktopost](https://github.com/fivetran/community_connectors/tree/main/oktopost)** - Sync social media exports from Oktopost BI API
- **[oura_ring](https://github.com/fivetran/community_connectors/tree/main/oura_ring)** - This example shows how to sync health and wellness data from the Oura Ring API v2 using Connector SDK. It syncs daily activity, sleep, readiness, stress, and heart rate data with incremental syncing, cursor-based pagination, automatic flattening of nested contributor objects, and date-range chunking for high-volume heart rate data. You need to provide your Oura Personal Access Token for this example to work.
- **[owasp_api_vulns](https://github.com/fivetran/community_connectors/tree/main/owasp_api_vulns)** - Sync OWASP API vulnerability data from NVD 2.0
- **[partech](https://github.com/fivetran/community_connectors/tree/main/partech)** - Sync POS data from Partech (formerly Punchh)
- **[pindrop](https://github.com/fivetran/community_connectors/tree/main/pindrop)** - Sync nightly report data from Pindrop
- **[prefect](https://github.com/fivetran/community_connectors/tree/main/prefect)** - Sync workflow orchestration data from Prefect Cloud
- **[prometheus](https://github.com/fivetran/community_connectors/tree/main/prometheus)** - Sync metrics and time series from Prometheus
- **[resend](https://github.com/fivetran/community_connectors/tree/main/resend)** - Sync email data from Resend API
- **[rillet](https://github.com/fivetran/community_connectors/tree/main/rillet)** - This example shows how to use the Connector SDK to integrate with Rillet's API and sync accounting data. You need to provide the API key for this example to work.
- **[s3_csv_validation](https://github.com/fivetran/community_connectors/tree/main/s3_csv_validation)** - Read and validate CSV files from Amazon S3
- **[sam_gov](https://github.com/fivetran/community_connectors/tree/main/sam_gov)** - Sync government contracting opportunities from SAM.gov
- **[sap_ariba](https://github.com/fivetran/community_connectors/tree/main/sap_ariba)** - Sync procurement data from SAP Ariba
- **[sendcloud](https://github.com/fivetran/community_connectors/tree/main/sendcloud)** - Sync shipment data from Sendcloud API
- **[sensor_tower](https://github.com/fivetran/community_connectors/tree/main/sensor_tower)** - Sync mobile app market intelligence from Sensor Tower
- **[sensource](https://github.com/fivetran/community_connectors/tree/main/sensource)** - Sync traffic and occupancy metrics from SenSource
- **[similarweb](https://github.com/fivetran/community_connectors/tree/main/similarweb)** - Sync website performance metrics from SimilarWeb
- **[smartsheets](https://github.com/fivetran/community_connectors/tree/main/smartsheets)** - Sync sheets and reports from Smartsheets
- **[snipeitapp](https://github.com/fivetran/community_connectors/tree/main/snipeitapp)** - Sync IT asset management data from Snipe-IT
- **[status_cake](https://github.com/fivetran/community_connectors/tree/main/status_cake)** - Sync uptime monitoring from StatusCake
- **[suitedash](https://github.com/fivetran/community_connectors/tree/main/suitedash)** - Sync CRM data from SuiteDash API
- **[supabase](https://github.com/fivetran/community_connectors/tree/main/supabase)** - Sync employee data from Supabase database
- **[talon_one](https://github.com/fivetran/community_connectors/tree/main/talon_one)** - Sync events data from Talon.One
- **[temporal_cloud](https://github.com/fivetran/community_connectors/tree/main/temporal_cloud)** - This example shows how to sync workflow execution and schedule data from Temporal Cloud using Connector SDK. It retrieves workflow metadata, as well as schedule configurations. The connector uses async operations for efficient data retrieval and implements streaming to handle large datasets. You need to provide your Temporal Cloud host, namespace, and API key for this example to work.
- **[toast](https://github.com/fivetran/community_connectors/tree/main/toast)** - Sync POS data from Toast
- **[tulip_interfaces](https://github.com/fivetran/community_connectors/tree/main/tulip_interfaces)** - Sync data from Tulip Tables
- **[veeva_vault/basic_auth](https://github.com/fivetran/community_connectors/tree/main/veeva_vault/basic_auth)** - Authenticate to Veeva Vault with basic auth
- **[veeva_vault/session_id_auth](https://github.com/fivetran/community_connectors/tree/main/veeva_vault/session_id_auth)** - Authenticate to Veeva Vault with session ID
- **[vercel](https://github.com/fivetran/community_connectors/tree/main/vercel)** - Sync deployment data from Vercel REST API
- **[weights_and_biases](https://github.com/fivetran/community_connectors/tree/main/weights_and_biases)** - This example shows how to sync machine learning experiment tracking data from Weights & Biases (W&B), including projects, runs (experiments), and artifacts (models and datasets), using the Fivetran Connector SDK The connector uses the W&B Python SDK to retrieve data and implements robust error handling. You need to provide your W&B API key and entity name for this example to work.
- **[zigpoll](https://github.com/fivetran/community_connectors/tree/main/zigpoll)** - Sync polling data from Zigpoll

</details>

## Documentation & Resources

- **[_template_connector](https://github.com/fivetran/community_connectors/tree/main/_template_connector)** - Reference template for building new connectors
- **[CONTRIBUTING.md](https://github.com/fivetran/community_connectors/blob/main/CONTRIBUTING.md)** - Guide for contributing to this repository
- **[PYTHON_CODING_STANDARDS.md](https://github.com/fivetran/community_connectors/blob/main/PYTHON_CODING_STANDARDS.md)** - Python coding standards and best practices
- **[FIVETRAN_CODING_PRINCIPLES.md](https://github.com/fivetran/community_connectors/blob/main/FIVETRAN_CODING_PRINCIPLES.md)** - Code review principles and PR guidelines
- **[Connector SDK Documentation](https://fivetran.com/docs/connectors/connector-sdk)** - Official SDK documentation
- **[Connector SDK Best Practices](https://fivetran.com/docs/connector-sdk/best-practices)** - Best practices guide

## Contributing

We welcome contributions from the community! Whether you want to add a new connector, improve existing ones, or fix bugs, your contributions are appreciated.

Please read our [CONTRIBUTING.md](https://github.com/fivetran/community_connectors/blob/main/CONTRIBUTING.md) guide for detailed information on:
- How to fork and create a pull request
- Coding standards and guidelines
- Testing requirements
- Review process

## Issue

Found an issue? Submit an [issue](https://github.com/fivetran/community_connectors/issues) and get connected to a Fivetran developer.

## Support

Learn how we [support Fivetran Connector SDK](https://fivetran.com/docs/connector-sdk#support).

## Additional Considerations

We provide examples to help you effectively use Fivetran's Connector SDK. While we've tested the code provided in these examples, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples.

Note that API calls made by your Connector SDK connection may count towards your service's API call allocation. Exceeding this limit could trigger rate limits, potentially impacting other uses of the source API.

It's important to choose the right design pattern for your target API. Using an inappropriate pattern may lead to data integrity issues. We recommend that you review all our examples carefully to select the one that best suits your target API. Keep in mind that some APIs may not support patterns for which we currently have examples.

As with other new connectors, SDK connectors have a [14-day trial period](https://fivetran.com/docs/getting-started/free-trials#newconnectorfreeuseperiod) during which your usage counts towards free [MAR](https://fivetran.com/docs/usage-based-pricing). After the 14-day trial period, your usage counts towards paid MAR. To avoid incurring charges, pause or delete any connections you created to run these examples before the trial ends.

## Maintenance

The `community_connectors` repository is actively maintained by Fivetran Developers. Reach out to our [Support team](https://support.fivetran.com/hc/en-us) for any inquiries.

## License

This project is licensed under the MIT License - see the [LICENSE](https://github.com/fivetran/community_connectors/blob/main/LICENSE) file for details.
