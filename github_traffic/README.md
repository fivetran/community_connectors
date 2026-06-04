# GitHub Repository Traffic Connector Example

## Connector overview

This connector syncs GitHub repository traffic data using the GitHub REST API. It collects repository views (count and unique visitors), repository clones (count and unique cloners), top referral sources, and top popular content paths. It demonstrates how to work with GitHub's traffic analytics endpoints, handle limited historical data (14 days), and use Personal Access Token authentication.

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
fivetran init --template github_traffic
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features

- Syncs repository views, clones, referrers, and popular content paths
- Uses Personal Access Token authentication
- Handles GitHub's 14-day traffic data window
- Supports multiple repositories in a single sync

## Configuration file

Update the `configuration.json` file with your GitHub Personal Access Token and the repositories you want to monitor:

```json
{
  "personal_access_token": "YOUR_GITHUB_PERSONAL_ACCESS_TOKEN",
  "repositories": "owner/repository1, owner/repository2"
}
```

> Note: When submitting connector code as a [Community Connector](https://github.com/fivetran/community_connectors/tree/main) in the open-source [Connector SDK repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

This connector uses only pre-installed packages in the Fivetran environment.

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

This connector uses [GitHub Personal Access Token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens) authentication. The token must have Read access to administration and metadata.

The Personal Access Token used must have the `repo` scope to access traffic data for private repositories. For public repositories, only the `public_repo` scope is needed.

The GitHub API has rate limits. For authenticated requests, the rate limit is 5,000 requests per hour.

## Data handling

The traffic data endpoints specifically (views, clones, referrers, paths) only return data for the past 14 days.

## Tables created

### repository_views

Contains daily view counts for each repository.

| Column      | Type         | Description                           |
|-------------|--------------|---------------------------------------|
| repository  | STRING       | Repository name in owner/repo format  |
| timestamp   | UTC_DATETIME | Date of the view data                 |
| count       | INT          | Total view count for the day          |
| uniques     | INT          | Unique visitor count for the day      |

### repository_clones

Contains daily clone counts for each repository.

| Column      | Type         | Description                           |
|-------------|--------------|---------------------------------------|
| repository  | STRING       | Repository name in owner/repo format  |
| timestamp   | UTC_DATETIME | Date of the clone data                |
| count       | INT          | Total clone count for the day         |
| uniques     | INT          | Unique cloner count for the day       |

### repository_referrers

Contains top referral sources for each repository.

| Column     | Type         | Description                               |
|------------|--------------|-------------------------------------------|
| repository | STRING       | Repository name in owner/repo format      |
| referrer   | STRING       | Referral source (e.g., "google.com")      |
| count      | INT          | Total views from this referrer            |
| uniques    | INT          | Unique visitors from this referrer        |
| fetch_date | NAIVE_DATE   | Date when data was collected from the API |

### repository_paths

Contains top content paths for each repository.

| Column       | Type         | Description                                |
|--------------|--------------|--------------------------------------------|
| repository   | STRING       | Repository name in owner/repo format       |
| path         | STRING       | Path to the content (e.g., "/README.md")   |
| title        | STRING       | Title of the content                       |
| count        | INT          | Total views for this path                  |
| uniques      | INT          | Unique visitors for this path              |
| fetch_date   | NAIVE_DATE   | Date when data was collected from the API  |

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
