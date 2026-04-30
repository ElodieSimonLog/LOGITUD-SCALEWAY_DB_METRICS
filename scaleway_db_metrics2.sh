#!/usr/bin/env bash
# =============================================================================
# scaleway_db_metrics.sh (clean version)
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SCW_SECRET_KEY="${SCW_SECRET_KEY:-}"
SCW_REGION="${SCW_REGION:-}"

DB_INSTANCES="${DB_INSTANCES:-}"

PUSHGATEWAY_URL="${PUSHGATEWAY_URL:-http://localhost:9091}"
PUSHGATEWAY_JOB="${PUSHGATEWAY_JOB:-scaleway_db}"

SCRAPE_INTERVAL="${SCRAPE_INTERVAL:-300}"
PSQL_CONNECT_TIMEOUT="${PSQL_CONNECT_TIMEOUT:-5}"

DB_AGGREGATE_THRESHOLD="${DB_AGGREGATE_THRESHOLD:-50}"
DB_TOP_N="${DB_TOP_N:-20}"

SCW_API_BASE="https://api.scaleway.com/rdb/v1/regions/${SCW_REGION}/instances"

REQUIRED_VARS=(SCW_SECRET_KEY SCW_REGION DB_INSTANCES PUSHGATEWAY_URL)

for var in "${REQUIRED_VARS[@]}"; do
  [[ -z "${!var:-}" ]] && { echo "[ERROR] Missing $var" >&2; exit 1; }
done

for cmd in curl jq psql; do
  command -v "$cmd" >/dev/null 2>&1 || { echo "[ERROR] Missing $cmd" >&2; exit 1; }
done

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
log()  { echo "[$(date '+%F %T')] $*"; }
warn() { echo "[$(date '+%F %T')] [WARN] $*"; }
err()  { echo "[$(date '+%F %T')] [ERROR] $*" >&2; }

# ---------------------------------------------------------------------------
# Metrics buffer
# ---------------------------------------------------------------------------
BUFFER=""

metric_header() {
  BUFFER+="# HELP $1 $3"$'\n'"# TYPE $1 $2"$'\n'
}

add_metric() {
  BUFFER+="$1{$3} $2"$'\n'
}

# ---------------------------------------------------------------------------
# psql wrapper
# ---------------------------------------------------------------------------
pg() {
  PGCONNECT_TIMEOUT="${PSQL_CONNECT_TIMEOUT}" PGPASSWORD="${PGPASSWORD}" \
  psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDB" \
    -t -A -F '|' "$@" 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# API Scaleway
# ---------------------------------------------------------------------------
collect_api_instance() {
  local instance_id="$1" env="$2"
  local labels="env=\"${env}\",instance_id=\"${instance_id}\",region=\"${SCW_REGION}\""

  local response http
  response=$(curl -s -w "|%{http_code}" \
    -H "X-Auth-Token: ${SCW_SECRET_KEY}" \
    "${SCW_API_BASE}/${instance_id}")

  http="${response##*|}"
  response="${response%|*}"

  [[ "$http" != "200" ]] && response="{}"

  local status vol_size ha max_conn

  status=$(echo "$response" | jq -r '.status // "unknown"')
  [[ "$status" == "ready" ]] && add_metric "scaleway_db_instance_ready" "1" "$labels" \
                            || add_metric "scaleway_db_instance_ready" "0" "$labels"

  vol_size=$(echo "$response" | jq -r '.volume.size // 0')
  [[ "$vol_size" =~ ^[0-9]+$ ]] && add_metric "scaleway_db_instance_volume_size_bytes" "$vol_size" "$labels"

  ha=$(echo "$response" | jq -r '.is_ha_cluster // false')
  [[ "$ha" == "true" ]] && add_metric "scaleway_db_instance_ha" "1" "$labels" \
                        || add_metric "scaleway_db_instance_ha" "0" "$labels"

  max_conn=$(echo "$response" | jq -r '
    .settings[]? | select(.name=="max_connections") | .value // empty
  ')

  [[ "$max_conn" =~ ^[0-9]+$ ]] && add_metric "scaleway_db_instance_max_connections" "$max_conn" "$labels"

  log "API instance $instance_id OK"
}

# ---------------------------------------------------------------------------
# Aggregated mode
# ---------------------------------------------------------------------------
collect_aggregated() {
  local instance_id="$1" env="$2"
  local labels="env=\"${env}\",instance_id=\"${instance_id}\""

  log "  mode: aggregated"

  local conn_data
  conn_data=$(pg -c "
    SELECT
      COUNT(*),
      COUNT(*) FILTER (WHERE state='active'),
      COUNT(*) FILTER (WHERE state='idle'),
      COUNT(*) FILTER (WHERE wait_event_type='Lock')
    FROM pg_stat_activity
    WHERE datname NOT IN ('postgres','rdb','');
  ")

  IFS='|' read -r total active idle waiting <<< "$conn_data"

  [[ "$total"   =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_connections_total"   "$total"   "$labels"
  [[ "$active"  =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_connections_active"  "$active"  "$labels"
  [[ "$idle"    =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_connections_idle"    "$idle"    "$labels"
  [[ "$waiting" =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_connections_waiting" "$waiting" "$labels"

  # max_connections + ratio
  local max_conn
  max_conn=$(pg -c "SELECT setting::int FROM pg_settings WHERE name='max_connections';")

  [[ "$max_conn" =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_max_connections" "$max_conn" "$labels"

  if [[ "$max_conn" =~ ^[0-9]+$ ]] && (( max_conn > 0 )); then
    local ratio
    ratio=$(echo "scale=4; $total / $max_conn" | bc)
    add_metric "scaleway_db_pg_connections_ratio" "$ratio" "$labels"
  fi

  # tailles par base
  local db_sizes
  db_sizes=$(pg -c "
    SELECT datname, pg_database_size(datname)
    FROM pg_database
    WHERE datistemplate=false
    ORDER BY 2 DESC
    LIMIT ${DB_TOP_N};
  ")

  while IFS='|' read -r db size; do
    [[ "$size" =~ ^[0-9]+$ ]] && \
      add_metric "scaleway_db_pg_size_bytes" "$size" "$labels,db=\"${db}\""
  done <<< "$db_sizes"
}

# ---------------------------------------------------------------------------
# Detailed mode
# ---------------------------------------------------------------------------
collect_detailed() {
  local instance_id="$1" env="$2" db="$3"
  local labels="env=\"${env}\",instance_id=\"${instance_id}\",db=\"${db}\""

  export PGDB="$db"

  local size
  size=$(pg -c "SELECT pg_database_size(current_database());")
  [[ "$size" =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_size_bytes" "$size" "$labels"

  local conn_data
  conn_data=$(pg -c "
    SELECT
      COUNT(*),
      COUNT(*) FILTER (WHERE state='active'),
      COUNT(*) FILTER (WHERE state='idle'),
      COUNT(*) FILTER (WHERE wait_event_type='Lock')
    FROM pg_stat_activity;
  ")

  IFS='|' read -r total active idle waiting <<< "$conn_data"

  [[ "$total"   =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_connections_total"   "$total"   "$labels"
  [[ "$active"  =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_connections_active"  "$active"  "$labels"
  [[ "$idle"    =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_connections_idle"    "$idle"    "$labels"
  [[ "$waiting" =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_connections_waiting" "$waiting" "$labels"

  # max_connections + ratio
  local max_conn
  max_conn=$(pg -c "SELECT setting::int FROM pg_settings WHERE name='max_connections';")

  [[ "$max_conn" =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_max_connections" "$max_conn" "$labels"

  if [[ "$max_conn" =~ ^[0-9]+$ ]] && (( max_conn > 0 )); then
    local ratio
    ratio=$(echo "scale=4; $total / $max_conn" | bc)
    add_metric "scaleway_db_pg_connections_ratio" "$ratio" "$labels"
  fi
}

# ---------------------------------------------------------------------------
# Pushgateway
# ---------------------------------------------------------------------------
push_metrics() {
  local url="${PUSHGATEWAY_URL}/metrics/job/${PUSHGATEWAY_JOB}"

  local code
  code=$(printf "%s" "$BUFFER" | curl -s -o /dev/null -w "%{http_code}" --data-binary @- -X PUT "$url")

  [[ "$code" == 2* ]] && log "Push OK" || err "Push failed HTTP $code"
}

# ---------------------------------------------------------------------------
# Scrape
# ---------------------------------------------------------------------------
scrape() {
  BUFFER=""
  local start end duration

  start=$(date +%s)

  metric_header "scaleway_db_instance_ready"              "gauge" "Instance ready (1=ok, 0=not ready)"
  metric_header "scaleway_db_instance_volume_size_bytes"  "gauge" "Volume size in bytes"
  metric_header "scaleway_db_instance_ha"                 "gauge" "HA enabled (1=yes, 0=no)"
  metric_header "scaleway_db_instance_max_connections"    "gauge" "Max connections configured on the instance"
  metric_header "scaleway_db_pg_connections_total"        "gauge" "Total open connections"
  metric_header "scaleway_db_pg_connections_active"       "gauge" "Active connections (executing a query)"
  metric_header "scaleway_db_pg_connections_idle"         "gauge" "Idle connections (open but doing nothing)"
  metric_header "scaleway_db_pg_connections_waiting"      "gauge" "Connections waiting for a lock"
  metric_header "scaleway_db_pg_max_connections"          "gauge" "Max connections read from PostgreSQL"
  metric_header "scaleway_db_pg_connections_ratio"        "gauge" "Ratio of used connections vs max (0 to 1)"
  metric_header "scaleway_db_pg_size_bytes"               "gauge" "Database size in bytes"
  metric_header "scaleway_db_last_scrape_duration_seconds" "gauge" "Duration of the last scrape in seconds"
  metric_header "scaleway_db_last_scrape_timestamp"       "gauge" "Unix timestamp of the last scrape"

  local count
  count=$(echo "$DB_INSTANCES" | jq 'length')

  (( count == 0 )) && { log "No instances"; return; }

  for i in $(seq 0 $((count-1))); do
    local inst id env host port user pass

    inst=$(echo "$DB_INSTANCES" | jq -r ".[$i]")
    id=$(echo "$inst"   | jq -r '.id')
    env=$(echo "$inst"  | jq -r '.env')
    host=$(echo "$inst" | jq -r '.host')
    port=$(echo "$inst" | jq -r '.port')
    user=$(echo "$inst" | jq -r '.user')
    pass=$(echo "$inst" | jq -r '.pass')

    log "Instance $env / $id"

    collect_api_instance "$id" "$env"

    export PGHOST="$host" PGPORT="$port" PGUSER="$user" PGPASSWORD="$pass" PGDB="postgres"

    mapfile -t dbs < <(
      psql -t -A -c "SELECT datname FROM pg_database WHERE datistemplate=false;" 2>/dev/null || true
    )

    if (( ${#dbs[@]} == 0 )); then
      warn "psql unreachable for $env / $id"
      continue
    fi

    if (( ${#dbs[@]} >= DB_AGGREGATE_THRESHOLD )); then
      collect_aggregated "$id" "$env"
    else
      for db in "${dbs[@]}"; do
        [[ -z "$db" ]] && continue
        collect_detailed "$id" "$env" "$db"
      done
    fi
  done

  end=$(date +%s)
  duration=$((end - start))

  BUFFER+="scaleway_db_last_scrape_duration_seconds $duration"$'\n'
  BUFFER+="scaleway_db_last_scrape_timestamp $end"$'\n'

  push_metrics
  log "Scrape done in ${duration}s"
}

# ---------------------------------------------------------------------------
# Loop
# ---------------------------------------------------------------------------
trap 'log "Stopping"' EXIT INT TERM

log "Starting scraper..."
while true; do
  scrape
  sleep "$SCRAPE_INTERVAL"
done