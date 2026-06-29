#!/bin/bash

set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

echo "Adding IBM i Access ODBC Driver repository..."
curl -fsSL https://public.dhe.ibm.com/software/ibmi/products/odbc/debs/dists/1.1.0/ibmi-acs-1.1.0.list \
  | tee /etc/apt/sources.list.d/ibmi-acs-1.1.0.list

echo "Installing IBM i Access ODBC Driver and unixODBC..."
apt-get -qq update >/dev/null || true
apt-get -qq install -y ibm-iaccess unixodbc unixodbc-dev >/dev/null

echo "Verifying driver registration..."
odbcinst -q -d -n "IBM i Access ODBC Driver"

echo "IBM i Access ODBC Driver installation complete."
