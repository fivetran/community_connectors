#!/bin/bash
# Installs the JRE required by jaydebeapi/JPype to load AVEVA's PI SQL Client
# JDBC driver inside the Hosted Connector SDK's Linux container.
#
# The Connector SDK runtime executes this script as root, before installing
# this connector's Python dependencies from requirements.txt (see
# connector_sdk_runner/init.sh in the fivetran/engineering monorepo).
set -euo pipefail

JAR_PATH="$(dirname "$0")/PIJDBCDriver.jar"

if [ ! -f "$JAR_PATH" ]; then
  echo "ERROR: $JAR_PATH not found." >&2
  echo "AVEVA's PI SQL Client JDBC driver is licensed software distributed only" >&2
  echo "through the AVEVA/OSIsoft customer support portal — it cannot be" >&2
  echo "committed to this public repository. Download PIJDBCDriver.jar from" >&2
  echo "your portal account and place it at drivers/PIJDBCDriver.jar before" >&2
  echo "packaging this connector with 'fivetran deploy'." >&2
  exit 1
fi

apt-get update
apt-get install -y --no-install-recommends default-jre-headless
rm -rf /var/lib/apt/lists/*

echo "JRE installed: $(java -version 2>&1 | head -1)"
