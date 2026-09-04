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

For SDK installation and setup, visit the main [Fivetran Connector SDK repository](https://github.com/fivetran/connector_sdk).

<details class="details-heading" open="open">
<summary>📋 Full List of Connectors</summary>

### Databases

- **Apache Druid (PyDruid)** ([apache_druid/using_pydruid](https://github.com/fivetran/community_connectors/tree/main/apache_druid/using_pydruid)) - This example demonstrates syncing from Apache Druid using the PyDruid library for native query capabilities, retry logic with exponential backoff, and optimized data retrieval for enhanced performance.
- **Apache Druid (SQL API)** ([apache_druid/using_sql](https://github.com/fivetran/community_connectors/tree/main/apache_druid/using_sql)) - This example shows how to sync data from Apache Druid datasources using Druid's SQL API with time-based pagination and incremental sync to efficiently replicate event and analytics data.
- **Apache HBase** ([apache_hbase](https://github.com/fivetran/community_connectors/tree/main/apache_hbase)) - Connect and sync data from Apache HBase using happybase and thrift libraries
- **Apache Hive (PyHive)** ([apache_hive/using_pyhive](https://github.com/fivetran/community_connectors/tree/main/apache_hive/using_pyhive)) - Sync data from Apache Hive using PyHive
- **Apache Hive (SQLAlchemy)** ([apache_hive/using_sqlalchemy](https://github.com/fivetran/community_connectors/tree/main/apache_hive/using_sqlalchemy)) - Sync data from Apache Hive using SQLAlchemy with PyHive
- **ArangoDB** ([arango_db](https://github.com/fivetran/community_connectors/tree/main/arango_db)) - Sync document and edge collections from ArangoDB multi-model database
- **AVEVA PI** ([aveva_pi](https://github.com/fivetran/community_connectors/tree/main/aveva_pi)) - Sync asset hierarchy and time-series data from AVEVA PI (formerly OSIsoft PI) via PI Web API
- **Cassandra** ([cassandra](https://github.com/fivetran/community_connectors/tree/main/cassandra)) - Connect and sync data from Cassandra database
- **ClickHouse** ([clickhouse](https://github.com/fivetran/community_connectors/tree/main/clickhouse)) - Sync data from ClickHouse database
- **Couchbase Capella** ([couchbase_capella](https://github.com/fivetran/community_connectors/tree/main/couchbase_capella)) - Sync data from Couchbase Capella
- **Couchbase Magma** ([couchbase_magma](https://github.com/fivetran/community_connectors/tree/main/couchbase_magma)) - Sync data from self-managed Couchbase Server using Magma storage
- **Dgraph** ([dgraph](https://github.com/fivetran/community_connectors/tree/main/dgraph)) - Sync e-commerce product catalog from Dgraph graph databases
- **AWS DocumentDB** ([documentdb](https://github.com/fivetran/community_connectors/tree/main/documentdb)) - Connect to AWS DocumentDB and sync collections (Hybrid Deployment compatible)
- **DolphinDB** ([dolphin_db](https://github.com/fivetran/community_connectors/tree/main/dolphin_db)) - Sync data from DolphinDB database
- **DragonflyDB** ([dragonfly_db](https://github.com/fivetran/community_connectors/tree/main/dragonfly_db)) - Sync high-performance in-memory data from DragonflyDB
- **EHI (Simple)** ([ehi/simple_ehi](https://github.com/fivetran/community_connectors/tree/main/ehi/simple_ehi)) - This example shows how to sync EHI tables such as Caboodle using Connector SDK. It connects to Microsoft SQL server to sync the data. You need to provide your server credentials for this example to work.
- **EHI (High Volume)** ([ehi/high_volume_ehi](https://github.com/fivetran/community_connectors/tree/main/ehi/high_volume_ehi)) - High-volume Microsoft SQL Server connector for syncing high volume EHI tables using keyset and offset pagination with parallel processing
- **Firebird** ([firebird_db](https://github.com/fivetran/community_connectors/tree/main/firebird_db)) - Sync data from Firebird DB
- **Greenplum** ([greenplum_db](https://github.com/fivetran/community_connectors/tree/main/greenplum_db)) - Sync data from Greenplum database
- **IBM Db2** ([ibm_db2](https://github.com/fivetran/community_connectors/tree/main/ibm/ibm_db2)) - This example shows how to connect and sync data from IBM Db2 using Connector SDK. It uses the `ibm_db` library to connect to the database and fetch data.
- **IBM Db2 (Log-Based Replication)** ([ibm_db2_log_based_replication](https://github.com/fivetran/community_connectors/tree/main/ibm/ibm_db2_log_based_replication)) - This example demonstrates log-based Change Data Capture (CDC) for IBM Db2 using the ASN SQL Replication framework. The `asncap` daemon reads the Db2 transaction log and writes every INSERT, UPDATE, and DELETE to a Change Data table; the connector reads exclusively from that table after the initial load, making this genuine log-based replication without polling the source.
- **IBM Db2 for i** ([ibm_db2i](https://github.com/fivetran/community_connectors/tree/main/ibm/ibm_db2i)) - This example shows how to connect and sync data from IBM Db2 for i (IBM i / AS400) using Connector SDK. It uses the `pyodbc` library with the IBM i Access ODBC Driver to connect to the database and fetch data.
- **IBM Informix (ibm_db)** ([ibm_informix_using_ibm_db](https://github.com/fivetran/community_connectors/tree/main/ibm/ibm_informix_using_ibm_db)) - This example shows how to connect and sync data from IBM Informix using Connector SDK. This example uses the `ibm_db` library to connect to the Informix database and fetch data.
- **IBM Informix (JayDeBeApi)** ([ibm_informix_using_jaydebeapi](https://github.com/fivetran/community_connectors/tree/main/ibm/ibm_informix_using_jaydebeapi)) - This example shows how to connect and sync data from IBM Informix using Connector SDK. This example uses the `jaydebeapi` library with external JDBC Informix driver, using `installation.sh` file in the `drivers` folder, to connect to the Informix database and fetch data.
- **JanusGraph** ([janus_graph](https://github.com/fivetran/community_connectors/tree/main/janus_graph)) - This example shows how to sync data from the Janus Graph database using the Connector SDK. You need to provide your Janus Graph credentials for this example to work.
- **InfluxDB** ([influx_db](https://github.com/fivetran/community_connectors/tree/main/influx_db)) - Sync time-series data from InfluxDB
- **Neo4j** ([neo4j](https://github.com/fivetran/community_connectors/tree/main/neo4j)) - Extract data from Neo4j graph databases
- **QuestDB** ([quest_db](https://github.com/fivetran/community_connectors/tree/main/quest_db)) - Sync high-performance time series data from QuestDB
- **RavenDB** ([raven_db](https://github.com/fivetran/community_connectors/tree/main/raven_db)) - Sync document data from RavenDB NoSQL database
- **Redis** ([redis](https://github.com/fivetran/community_connectors/tree/main/redis)) - Sync gaming leaderboards and player statistics from Redis
- **RethinkDB** ([rethink_db](https://github.com/fivetran/community_connectors/tree/main/rethink_db)) - Sync data from RethinkDB real-time database
- **SAP HANA** ([sap_hana_sql](https://github.com/fivetran/community_connectors/tree/main/sap_hana_sql)) - Connect to SAP HANA SQL Server using hdbcli
- **SQL Server** ([sql_server](https://github.com/fivetran/community_connectors/tree/main/sql_server)) - Connect to SQL Server using pyodbc
- **Sybase IQ** ([sybase_iq](https://github.com/fivetran/community_connectors/tree/main/sybase_iq)) - This example demonstrates how to connect to Sybase IQ database using the `FreeTDS` driver and `PyODBC` by writing a `installation.sh` file in the `drivers` folder.
- **Sybase ASE** ([sybase_ase](https://github.com/fivetran/community_connectors/tree/main/sybase_ase)) - This example demonstrates how to connect to Sybase ASE database using the `FreeTDS` driver and `PyODBC` by writing a `installation.sh` file in the `drivers` folder.
- **Teradata Vantage** ([teradata](https://github.com/fivetran/community_connectors/tree/main/teradata)) - Sync data from Teradata Vantage database
- **TiDB** ([tidb](https://github.com/fivetran/community_connectors/tree/main/tidb)) - Incremental replication from TiDB databases
- **TimescaleDB** ([timescale_db](https://github.com/fivetran/community_connectors/tree/main/timescale_db)) - Sync time-series and vector data from TimescaleDB
- **YugabyteDB** ([yugabyte_db](https://github.com/fivetran/community_connectors/tree/main/yugabyte_db)) - Sync data from YugabyteDB distributed SQL database

### Cloud Data Warehouses

- **AWS Athena (Boto3)** ([aws_athena/using_boto3](https://github.com/fivetran/community_connectors/tree/main/aws_athena/using_boto3)) - Sync data from AWS Athena using Boto3
- **AWS Athena (SQLAlchemy with PyAthena)** ([aws_athena/using_sqlalchemy](https://github.com/fivetran/community_connectors/tree/main/aws_athena/using_sqlalchemy)) - Sync data from AWS Athena using SQLAlchemy with PyAthena
- **AWS DynamoDB (IAM Role Authentication)** ([aws_dynamo_db_authentication](https://github.com/fivetran/community_connectors/tree/main/aws_dynamo_db_authentication)) - Connect and sync data from AWS DynamoDB
- **AWS RDS Oracle** ([aws_rds_oracle](https://github.com/fivetran/community_connectors/tree/main/aws_rds_oracle)) - Connect and sync data from AWS Oracle
- **Redshift** ([redshift/simple_redshift_connector](https://github.com/fivetran/community_connectors/tree/main/redshift/simple_redshift_connector)) - Sync records from Redshift
- **Redshift (Large Data Volume)** ([redshift/large_data_volume](https://github.com/fivetran/community_connectors/tree/main/redshift/large_data_volume)) - Sync large data volumes from Redshift
- **Redshift (Using UNLOAD)** ([redshift/using_unload](https://github.com/fivetran/community_connectors/tree/main/redshift/using_unload)) - Sync data from Redshift using UNLOAD to S3
- **Delta Sharing** ([delta_sharing](https://github.com/fivetran/community_connectors/tree/main/delta_sharing)) - Connect and sync data from Databricks Delta Sharing (now OpenSharing): An Open Protocol for Secure Data Sharing       

### Message Queues & Streaming

- **Apache Pulsar** ([apache_pulsar](https://github.com/fivetran/community_connectors/tree/main/apache_pulsar)) - Fetch data from Apache Pulsar topics with Reader API
- **Google Cloud Pub/Sub** ([gcp_pub_sub](https://github.com/fivetran/community_connectors/tree/main/gcp_pub_sub)) - Sync data from Google Cloud Pub/Sub
- **RabbitMQ** ([rabbitmq](https://github.com/fivetran/community_connectors/tree/main/rabbitmq)) - Sync messages from RabbitMQ queues
- **Solace** ([solace](https://github.com/fivetran/community_connectors/tree/main/solace)) - Sync messages from Solace queue

### AI and Connector SDK

- **Databricks Best Buy Retail Intelligence** ([databricks/databricks-fm-bestbuy-retail-intelligence](https://github.com/fivetran/community_connectors/tree/main/databricks/databricks-fm-bestbuy-retail-intelligence)) - Sync Best Buy product catalog data and enrich it with Databricks `ai_query()` retail intelligence for pricing, positioning, and sentiment analysis.
- **Databricks CPSC Product Safety Intelligence** ([databricks/databricks-fm-cpsc-product-safety-intelligence](https://github.com/fivetran/community_connectors/tree/main/databricks/databricks-fm-cpsc-product-safety-intelligence)) - Sync CPSC product recall data and enrich incidents with Databricks `ai_query()` multi-agent product safety analysis.
- **Databricks FDA Drug Label Intelligence** ([databricks/databricks-fm-fda-drug-label-intelligence](https://github.com/fivetran/community_connectors/tree/main/databricks/databricks-fm-fda-drug-label-intelligence)) - Sync OpenFDA drug label data and enrich package inserts with Databricks `ai_query()` analysis and optional Genie Space creation.
- **Databricks FDA FAERS Pharmacovigilance Intelligence** ([databricks/databricks-fm-fda-faers-pv-intelligence](https://github.com/fivetran/community_connectors/tree/main/databricks/databricks-fm-fda-faers-pv-intelligence)) - Sync FDA FAERS adverse event reports and enrich serious events with Databricks `ai_query()` pharmacovigilance debate analysis.
- **Databricks FHIR Healthcare Intelligence** ([databricks/databricks-fm-fhir-healthcare-intelligence](https://github.com/fivetran/community_connectors/tree/main/databricks/databricks-fm-fhir-healthcare-intelligence)) - Sync clinical data from a FHIR R4 server and enriches it with AI-powered hybrid analysis using Databricks `ai_query()`.
- **Databricks NOAA Weather Risk Intelligence** ([databricks/databricks-fm-noaa-weather-risk-intelligence](https://github.com/fivetran/community_connectors/tree/main/databricks/databricks-fm-noaa-weather-risk-intelligence)) - Sync NOAA severe weather alerts and use Databricks `ai_query()` for agent-driven discovery and emergency risk analysis.
- **Databricks SEC EDGAR Risk Intelligence** ([databricks/databricks-fm-sec-edgar-risk-intelligence](https://github.com/fivetran/community_connectors/tree/main/databricks/databricks-fm-sec-edgar-risk-intelligence)) - Sync SEC EDGAR filings and XBRL financial facts, then enrich them with Databricks `ai_query()` credit risk intelligence.
- **Databricks TVMaze Programming Intelligence** ([databricks/databricks-fm-tvmaze-programming-intelligence](https://github.com/fivetran/community_connectors/tree/main/databricks/databricks-fm-tvmaze-programming-intelligence)) - Sync TVMaze show metadata and enrich each show with Databricks `ai_query()` programming renewal debate analysis.
- **Snowflake Cortex Clinical Trial Intelligence** ([snowflake_cortex/snowflake-cortex-code-clinical-trial-intelligence](https://github.com/fivetran/community_connectors/tree/main/snowflake_cortex/snowflake-cortex-code-clinical-trial-intelligence)) - Sync ClinicalTrials.gov records and enrich them with Snowflake Cortex Agent clinical trial landscape intelligence.
- **Snowflake Cortex NHTSA Safety Intelligence** ([snowflake_cortex/snowflake-cortex-code-nhtsa-safety-intelligence](https://github.com/fivetran/community_connectors/tree/main/snowflake_cortex/snowflake-cortex-code-nhtsa-safety-intelligence)) - Sync NHTSA recalls, complaints, and vehicle specs, then use Snowflake Cortex Agent for vehicle safety discovery and analysis.
- **Snowflake Cortex NVD CVE Threat Intelligence** ([snowflake_cortex/snowflake-cortex-code-nvd-cve-threat-intelligence](https://github.com/fivetran/community_connectors/tree/main/snowflake_cortex/snowflake-cortex-code-nvd-cve-threat-intelligence)) - Sync NVD CVE records and enrich vulnerabilities with Snowflake Cortex multi-agent threat intelligence.
- **Snowflake Cortex Hacker News** ([snowflake_cortex/snowflake-cortex-hacker-news](https://github.com/fivetran/community_connectors/tree/main/snowflake_cortex/snowflake-cortex-hacker-news)) - Sync Hacker News top stories and enrich them with Snowflake Cortex sentiment and topic classification.
- **Snowflake Cortex Livestock Weather Intelligence** ([snowflake_cortex/snowflake-cortex-livestock-weather-intelligence](https://github.com/fivetran/community_connectors/tree/main/snowflake_cortex/snowflake-cortex-livestock-weather-intelligence)) - Sync farm weather forecasts and enrich them with Snowflake Cortex livestock health risk assessments.

### SaaS & APIs

- **Amazon Video Central** ([amazon_video_central](https://github.com/fivetran/community_connectors/tree/main/amazon_video_central)) - Sync report data from Amazon Video Central API
- **Awardco** ([awardco](https://github.com/fivetran/community_connectors/tree/main/awardco)) - Sync data from Awardco rewards platform
- **Beehiiv** ([beehiiv](https://github.com/fivetran/community_connectors/tree/main/beehiiv)) - Sync newsletter data from the beehiiv API including publications, subscriptions, posts, email blasts, automations, and engagement metrics
- **Better Stack** ([betterstack](https://github.com/fivetran/community_connectors/tree/main/betterstack)) - Sync uptime monitoring data from Better Stack
- **Bright Data Web Scraper** ([bright_data_scrape](https://github.com/fivetran/community_connectors/tree/main/bright_data_scrape)) - Sync scraped web page content and extracted fields from the Bright Data Web Scraper API
- **Bright Data Web Unlocker** ([bright_data_unlocker](https://github.com/fivetran/community_connectors/tree/main/bright_data_unlocker)) - Sync unlocked web page content for one or more URLs from the Bright Data Web Unlocker API
- **CallMiner** ([callminer](https://github.com/fivetran/fivetran_csdk_connectors/tree/main/callminer)) - Sync CallMiner Bulk Export data using OAuth2 authentication, export job polling, archive extraction, and per-data-type incremental state tracking
- **Checkly** ([checkly](https://github.com/fivetran/community_connectors/tree/main/checkly)) - Sync monitoring check data and analytics from Checkly
- **Clerk** ([clerk](https://github.com/fivetran/community_connectors/tree/main/clerk)) - Sync user data from Clerk authentication
- **Common Paper** ([commonpaper](https://github.com/fivetran/community_connectors/tree/main/commonpaper)) - Sync agreement data from Common Paper
- **Courier** ([courier](https://github.com/fivetran/community_connectors/tree/main/courier)) - Sync notifications data from Courier multi-channel platform
- **Customer Thermometer** ([customer_thermometer](https://github.com/fivetran/community_connectors/tree/main/customer_thermometer)) - Sync customer feedback from Customer Thermometer
- **DataCamp** ([data_camp](https://github.com/fivetran/community_connectors/tree/main/data_camp)) - Sync course catalog from DataCamp LMS
- **Discord** ([discord](https://github.com/fivetran/community_connectors/tree/main/discord)) - Sync data from Discord
- **Docusign eSignature** ([docusign](https://github.com/fivetran/community_connectors/tree/main/docusign)) - Sync data from Docusign eSignature API
- **Elastic Email** ([elastic_email](https://github.com/fivetran/community_connectors/tree/main/elastic_email)) - Sync email marketing data from Elastic Email
- **Federal Register** ([federal_register](https://github.com/fivetran/community_connectors/tree/main/federal_register)) - Sync rules, proposed rules, notices, and presidential documents from the Federal Register API
- **Fleetio** ([fleetio](https://github.com/fivetran/community_connectors/tree/main/fleetio)) - Sync fleet management data from Fleetio
- **FRED** ([fred](https://github.com/fivetran/community_connectors/tree/main/fred)) - Sync economic data from Federal Reserve Economic Data (FRED)
- **GBIF** ([gbif](https://github.com/fivetran/community_connectors/tree/main/gbif)) - Sync species occurrence records from the GBIF (Global Biodiversity Information Facility) API
- **GitHub** ([github](https://github.com/fivetran/community_connectors/tree/main/github)) - Sync repository data, commits, and pull requests from GitHub
- **GitHub Repository Traffic** ([github_traffic](https://github.com/fivetran/community_connectors/tree/main/github_traffic)) - Sync GitHub repository traffic data
- **GLEIF** ([gleif](https://github.com/fivetran/community_connectors/tree/main/gleif)) - Sync Legal Entity Identifier (LEI) reference data from the GLEIF API
- **GNews** ([gnews](https://github.com/fivetran/community_connectors/tree/main/gnews)) - Sync news articles from GNews API
- **Google Trends** ([google_trends](https://github.com/fivetran/community_connectors/tree/main/google_trends)) - Sync search interest data from Google Trends
- **Goshippo** ([goshippo](https://github.com/fivetran/community_connectors/tree/main/goshippo)) - Sync shipment data from Goshippo API
- **greytHR** ([grey_hr](https://github.com/fivetran/community_connectors/tree/main/grey_hr)) - Sync HR data from greytHR API
- **Gumroad** ([gumroad](https://github.com/fivetran/community_connectors/tree/main/gumroad)) - Sync sales, products, and subscribers from Gumroad
- **Harness.io** ([harness_io](https://github.com/fivetran/community_connectors/tree/main/harness_io)) - Connect and sync data from Harness.io
- **Healthchecks.io** ([healthchecks](https://github.com/fivetran/community_connectors/tree/main/healthchecks)) - Sync health check monitoring from Healthchecks.io
- **HubSpot** ([hubspot](https://github.com/fivetran/community_connectors/tree/main/hubspot)) - Sync event data from HubSpot
- **Iterate** ([iterate](https://github.com/fivetran/community_connectors/tree/main/iterate)) - Sync NPS survey data from Iterate REST API
- **Keycloak** ([keycloak](https://github.com/fivetran/community_connectors/tree/main/keycloak)) - Sync IAM data from Keycloak Admin API
- **LeaveDates** ([leavedates](https://github.com/fivetran/community_connectors/tree/main/leavedates)) - Sync leave report data from LeaveDates API
- **MailerLite** ([mailerlite](https://github.com/fivetran/community_connectors/tree/main/mailerlite)) - Sync email marketing data from MailerLite
- **MasterTax** ([mastertax](https://github.com/fivetran/community_connectors/tree/main/mastertax)) - Sync data from MasterTax API
- **MeiliSearch** ([meilisearch](https://github.com/fivetran/community_connectors/tree/main/meilisearch)) - Sync index metadata and documents from MeiliSearch
- **Microsoft Excel** ([microsoft_excel](https://github.com/fivetran/community_connectors/tree/main/microsoft_excel)) - Sync data from Microsoft Excel files
- **Microsoft Intune** ([microsoft_intune](https://github.com/fivetran/community_connectors/tree/main/microsoft_intune)) - Retrieve managed devices from Microsoft Intune
- **n8n** ([n8n](https://github.com/fivetran/community_connectors/tree/main/n8n)) - Sync workflow automation data from n8n
- **Netlify** ([netlify](https://github.com/fivetran/community_connectors/tree/main/netlify)) - Sync sites, deploys, and forms from Netlify API
- **News API** ([newsapi](https://github.com/fivetran/community_connectors/tree/main/newsapi)) - Sync news articles from NewsAPI
- **NOAA** ([noaa](https://github.com/fivetran/community_connectors/tree/main/noaa)) - Sync weather observations from National Weather Service
- **NPPES NPI Registry** ([npi_registry](https://github.com/fivetran/community_connectors/tree/main/npi_registry)) - Sync healthcare provider data from NPPES NPI Registry
- **Accelo** ([oauth2_and_accelo_api_connector_multithreading_enabled](https://github.com/fivetran/community_connectors/tree/main/oauth2_and_accelo_api_connector_multithreading_enabled)) - Sync data from Accelo API with OAuth 2.0 and multithreading
- **OData API** ([odata_api](https://github.com/fivetran/community_connectors/tree/main/odata_api)) - Sync data from OData APIs (versions 2 and 4)
- **Oktopost** ([oktopost](https://github.com/fivetran/community_connectors/tree/main/oktopost)) - Sync social media exports from Oktopost BI API
- **Open-Meteo Marine Weather** ([open_meteo_marine_weather](https://github.com/fivetran/community_connectors/tree/main/open_meteo_marine_weather)) - Sync hourly and daily marine weather data (wave height, direction, period, swell, wind waves) from the Open-Meteo Marine Weather API
- **Oura Ring** ([oura_ring](https://github.com/fivetran/community_connectors/tree/main/oura_ring)) - This example shows how to sync health and wellness data from the Oura Ring API v2 using Connector SDK. It syncs daily activity, sleep, readiness, stress, and heart rate data with incremental syncing, cursor-based pagination, automatic flattening of nested contributor objects, and date-range chunking for high-volume heart rate data. You need to provide your Oura Personal Access Token for this example to work.
- **OWASP API Vulnerabilities** ([owasp_api_vulns](https://github.com/fivetran/community_connectors/tree/main/owasp_api_vulns)) - Sync OWASP API vulnerability data from NVD 2.0
- **Partech (Punchh)** ([partech](https://github.com/fivetran/community_connectors/tree/main/partech)) - Sync POS data from Partech (formerly Punchh)
- **Pindrop** ([pindrop](https://github.com/fivetran/community_connectors/tree/main/pindrop)) - Sync nightly report data from Pindrop
- **Prefect Cloud** ([prefect](https://github.com/fivetran/community_connectors/tree/main/prefect)) - Sync workflow orchestration data from Prefect Cloud
- **Prometheus** ([prometheus](https://github.com/fivetran/community_connectors/tree/main/prometheus)) - Sync metrics and time series from Prometheus
- **Resend** ([resend](https://github.com/fivetran/community_connectors/tree/main/resend)) - Sync email data from Resend API
- **Rillet** ([rillet](https://github.com/fivetran/community_connectors/tree/main/rillet)) - This example shows how to use the Connector SDK to integrate with Rillet's API and sync accounting data. You need to provide the API key for this example to work.
- **S3 CSV File Reader with Data Validation** ([s3_csv_validation](https://github.com/fivetran/community_connectors/tree/main/s3_csv_validation)) - Read and validate CSV files from Amazon S3
- **SAM.gov** ([sam_gov](https://github.com/fivetran/community_connectors/tree/main/sam_gov)) - Sync government contracting opportunities from SAM.gov
- **SAP Ariba** ([sap_ariba](https://github.com/fivetran/community_connectors/tree/main/sap_ariba)) - Sync procurement data from SAP Ariba
- **Sendcloud** ([sendcloud](https://github.com/fivetran/community_connectors/tree/main/sendcloud)) - Sync shipment data from Sendcloud API
- **Sensor Tower** ([sensor_tower](https://github.com/fivetran/community_connectors/tree/main/sensor_tower)) - Sync mobile app market intelligence from Sensor Tower
- **SenSource** ([sensource](https://github.com/fivetran/community_connectors/tree/main/sensource)) - Sync traffic and occupancy metrics from SenSource
- **SharePoint Multi-Site** ([sharepoint_multi_site_connector](https://github.com/fivetran/community_connectors/tree/main/sharepoint_multi_site_connector)) - Syncs CSV and Excel file data from multiple SharePoint Online sites using the Microsoft Graph API. Supports multi-sheet Excel workbooks, recursive folder traversal, incremental sync, and deletion handling.
- **BAI2 SFTP** ([sftp_connector/bai2_sftp_connector](https://github.com/fivetran/community_connectors/tree/main/sftp_connector/bai2_sftp_connector)) - Fetch BAI2-format cash management files from an SFTP server and load all transactions into a single destination table with incremental sync and structured ACH field parsing
- **Fixed-Width SFTP** ([sftp_connector/fixed_width_sftp_connector](https://github.com/fivetran/community_connectors/tree/main/sftp_connector/fixed_width_sftp_connector)) - Read 12 fixed-width files from 3 SFTP subdirectories (ELAN, CUP, LPL/DFM) into 12 destination tables with soft-delete purge logic and implied-decimal parsing
- **Similarweb** ([similarweb](https://github.com/fivetran/community_connectors/tree/main/similarweb)) - Sync website performance metrics from SimilarWeb
- **Smartsheet** ([smartsheets](https://github.com/fivetran/community_connectors/tree/main/smartsheets)) - Sync sheets and reports from Smartsheets
- **Snipe-IT** ([snipeitapp](https://github.com/fivetran/community_connectors/tree/main/snipeitapp)) - Sync IT asset management data from Snipe-IT
- **StatusCake** ([status_cake](https://github.com/fivetran/community_connectors/tree/main/status_cake)) - Sync uptime monitoring from StatusCake
- **SuiteDash** ([suitedash](https://github.com/fivetran/community_connectors/tree/main/suitedash)) - Sync CRM data from SuiteDash API
- **Supabase** ([supabase](https://github.com/fivetran/community_connectors/tree/main/supabase)) - Sync employee data from Supabase database
- **Talon.One** ([talon_one](https://github.com/fivetran/community_connectors/tree/main/talon_one)) - Sync events data from Talon.One
- **Temporal Cloud** ([temporal_cloud](https://github.com/fivetran/community_connectors/tree/main/temporal_cloud)) - This example shows how to sync workflow execution and schedule data from Temporal Cloud using Connector SDK. It retrieves workflow metadata, as well as schedule configurations. The connector uses async operations for efficient data retrieval and implements streaming to handle large datasets. You need to provide your Temporal Cloud host, namespace, and API key for this example to work.
- **Toast** ([toast](https://github.com/fivetran/community_connectors/tree/main/toast)) - Sync POS data from Toast
- **Tulip Interfaces** ([tulip_interfaces](https://github.com/fivetran/community_connectors/tree/main/tulip_interfaces)) - Sync data from Tulip Tables
- **Veeva Vault (Basic Authentication)** ([veeva_vault/basic_auth](https://github.com/fivetran/community_connectors/tree/main/veeva_vault/basic_auth)) - Authenticate to Veeva Vault with basic auth
- **Veeva Vault (Session Authentication)** ([veeva_vault/session_id_auth](https://github.com/fivetran/community_connectors/tree/main/veeva_vault/session_id_auth)) - Authenticate to Veeva Vault with session ID
- **Vercel** ([vercel](https://github.com/fivetran/community_connectors/tree/main/vercel)) - Sync deployment data from Vercel REST API
- **Oracle WMS** ([wms_oracle](https://github.com/fivetran/community_connectors/tree/main/wms_oracle)) - Sync warehouse management data from Oracle WMS REST API with incremental sync, historical backfill, and pre-cursor drift detection across 26 entities.
- **Weights & Biases** ([weights_and_biases](https://github.com/fivetran/community_connectors/tree/main/weights_and_biases)) - This example shows how to sync machine learning experiment tracking data from Weights & Biases (W&B), including projects, runs (experiments), and artifacts (models and datasets), using the Fivetran Connector SDK The connector uses the W&B Python SDK to retrieve data and implements robust error handling. You need to provide your W&B API key and entity name for this example to work.
- **Zigpoll** ([zigpoll](https://github.com/fivetran/community_connectors/tree/main/zigpoll)) - Sync polling data from Zigpoll

</details>

## Documentation & Resources

- **Template Connector** ([_template_connector](https://github.com/fivetran/community_connectors/tree/main/_template_connector)) - Reference template for building new connectors
- **Empty Project** ([empty-project](https://github.com/fivetran/community_connectors/tree/main/empty-project)) - Blank skeleton connector to start from scratch
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

Learn how we [support Fivetran Connector SDK](https://fivetran.com/docs/connector-sdk#supportandresourcesforconnectordevelopment).

## Additional Considerations

We provide examples to help you effectively use Fivetran's Connector SDK. While we've tested the code provided in these examples, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples.

Note that API calls made by your Connector SDK connection may count towards your service's API call allocation. Exceeding this limit could trigger rate limits, potentially impacting other uses of the source API.

It's important to choose the right design pattern for your target API. Using an inappropriate pattern may lead to data integrity issues. We recommend that you review all our examples carefully to select the one that best suits your target API. Keep in mind that some APIs may not support patterns for which we currently have examples.

As with other new connectors, SDK connectors have a [14-day trial period](https://fivetran.com/docs/getting-started/free-trials#newconnectorfreeuseperiod) during which your usage counts towards free [MAR](https://fivetran.com/docs/usage-based-pricing). After the 14-day trial period, your usage counts towards paid MAR. To avoid incurring charges, pause or delete any connections you created to run these examples before the trial ends.

## Maintenance

The `community_connectors` repository is actively maintained by Fivetran Developers. Reach out to our [Support team](https://support.fivetran.com/hc/en-us) for any inquiries.

## License

This project is licensed under the MIT License - see the [LICENSE](https://github.com/fivetran/community_connectors/blob/main/LICENSE) file for details.
