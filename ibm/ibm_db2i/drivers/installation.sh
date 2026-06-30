#!/bin/bash

set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEB_FILE="$SCRIPT_DIR/ibm-iaccess_1.1.0.29-1.0_amd64.deb"

echo "Installing unixODBC dependencies..."
apt-get -qq update >/dev/null
apt-get -qq install -y unixodbc unixodbc-dev >/dev/null

echo "Installing IBM i Access ODBC Driver from local package..."
dpkg -i "$DEB_FILE"

echo "Verifying driver registration..."
odbcinst -q -d -n "IBM i Access ODBC Driver"

echo "IBM i Access ODBC Driver installation complete."
