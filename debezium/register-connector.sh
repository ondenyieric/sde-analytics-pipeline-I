#!/bin/sh
# Waits for Kafka Connect (Debezium) to be ready, substitutes DB credentials
# from the environment into postgres-connector.json, and registers it.
# Run automatically by the `debezium-connector-init` service in
# docker-compose.yml; safe to re-run (PUT is idempotent).

set -eu

CONNECT_URL="http://debezium:8083"
: "${POSTGRES_USER:=pipeline}"
: "${POSTGRES_PASSWORD:=pipeline}"
: "${POSTGRES_DB:=pipeline}"
CONFIG_FILE="/debezium-setup/postgres-connector.json"

echo "Waiting for Kafka Connect at ${CONNECT_URL} ..."
until curl -s -o /dev/null -w "%{http_code}" "${CONNECT_URL}/connectors" | grep -q "200"; do
  sleep 3
done
echo "Kafka Connect is up."

# Substitute ${VAR} placeholders in the connector config with real values.
RENDERED=$(sed \
  -e "s/\${POSTGRES_USER}/${POSTGRES_USER}/g" \
  -e "s/\${POSTGRES_PASSWORD}/${POSTGRES_PASSWORD}/g" \
  -e "s/\${POSTGRES_DB}/${POSTGRES_DB}/g" \
  "${CONFIG_FILE}")

CONNECTOR_NAME=$(echo "${RENDERED}" | grep -o '"name": *"[^"]*"' | head -1 | sed 's/.*"name": *"\([^"]*\)".*/\1/')

echo "Registering connector: ${CONNECTOR_NAME}"

EXISTING_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${CONNECT_URL}/connectors/${CONNECTOR_NAME}")

if [ "${EXISTING_STATUS}" = "200" ]; then
  echo "Connector already exists, updating config (PUT) ..."
  # Extract just the inner "config": { ... } object using sed (no jq/python
  # in the curlimages/curl base image). The connector JSON is written with a
  # single top-level "config" object, so this brace-matching sed is safe for
  # this file's known shape.
  CONFIG_ONLY=$(echo "${RENDERED}" | sed -n '/"config": {/,/^  }/p' | sed '1s/.*"config": //' | sed '$s/}$/}/')
  curl -s -X PUT \
    -H "Content-Type: application/json" \
    --data "${CONFIG_ONLY}" \
    "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config" \
    -o /dev/stdout -w "\nHTTP %{http_code}\n"
else
  echo "Creating connector (POST) ..."
  curl -s -X POST \
    -H "Content-Type: application/json" \
    --data "${RENDERED}" \
    "${CONNECT_URL}/connectors" \
    -o /dev/stdout -w "\nHTTP %{http_code}\n"
fi

echo "Connector registration request sent."
