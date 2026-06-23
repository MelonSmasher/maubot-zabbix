#!/bin/sh
# Build a .mbp (maubot plugin bundle). The .mbp format is just a zip
# containing maubot.yaml + the plugin source + extra_files.
set -eu

PLUGIN_VERSION=$(awk '/^version:/ {print $2}' maubot.yaml)
OUTPUT="com.melonsmasher.zabbix-v${PLUGIN_VERSION}.mbp"

rm -f "${OUTPUT}"

zip -qr "${OUTPUT}" \
    maubot.yaml \
    zabbix.py \
    base-config.yaml

echo "Built: ${OUTPUT}"
