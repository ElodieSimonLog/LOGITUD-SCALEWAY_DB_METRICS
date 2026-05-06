#!/usr/bin/env bash
# =============================================================================
# scaleway_db_metrics.sh
#
# Collecte toutes les 5 min les métriques des bases PostgreSQL Scaleway
# et les pousse vers un Prometheus Pushgateway.
#
# Sources de données :
#   - API REST Scaleway  → infos statiques par instance (volume, status, max_conn, HA)
#   - psql direct        → métriques live (connexions, tailles, requêtes lentes)
#
# Deux modes selon le nombre de bases sur l'instance :
#   MODE AGRÉGÉ   (>= DB_AGGREGATE_THRESHOLD)  → 1 connexion/instance, top N tailles
#   MODE DÉTAILLÉ (< DB_AGGREGATE_THRESHOLD)   → 1 connexion/base, pg_stat_statements
#
# Instances sur réseau privé (172.x) : skippées en psql si injoignables,
# mais les infos API REST sont quand même collectées.
#
# Prérequis : curl, jq, psql
#
# Usage :
#   chmod +x scaleway_db_metrics.sh && ./scaleway_db_metrics.sh
#
# Port-forward Pushgateway :
#   kubectl port-forward svc/<pushgateway-svc> 9091:9091 -n grafana
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ -f "${SCRIPT_DIR}/.env" ]] && source "${SCRIPT_DIR}/.env"

# ---------------------------------------------------------------------------
# Configuration (surchargeables dans .env)
# ---------------------------------------------------------------------------
SCRAPE_INTERVAL="${SCRAPE_INTERVAL:-300}"
PUSHGATEWAY_URL="${PUSHGATEWAY_URL:-http://localhost:9091}"
PUSHGATEWAY_JOB="${PUSHGATEWAY_JOB:-scaleway_db}"
SCW_API_BASE="https://api.scaleway.com/rdb/v1/regions/${SCW_REGION}/instances"
DB_AGGREGATE_THRESHOLD="${DB_AGGREGATE_THRESHOLD:-50}"
DB_TOP_N="${DB_TOP_N:-20}"
PSQL_CONNECT_TIMEOUT="${PSQL_CONNECT_TIMEOUT:-5}"

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
REQUIRED_VARS=(SCW_SECRET_KEY SCW_REGION DB_INSTANCES PUSHGATEWAY_URL)
for var in "${REQUIRED_VARS[@]}"; do
  [[ -z "${!var:-}" ]] && { echo "[ERROR] Variable manquante : $var" >&2; exit 1; }
done
for cmd in curl jq psql; do
  command -v "$cmd" &>/dev/null || { echo "[ERROR] Commande manquante : $cmd" >&2; exit 1; }
done

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
log()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
warn() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [WARN] $*"; }
err()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $*" >&2; }

# ---------------------------------------------------------------------------
# Buffer Prometheus
# ---------------------------------------------------------------------------
BUFFER=""
metric_header() { BUFFER+="# HELP $1 $3"$'\n'"# TYPE $1 $2"$'\n'; }
add_metric()    { BUFFER+="$1{$2} $3"$'\n'; }

# ---------------------------------------------------------------------------
# Wrapper psql (utilise les variables d'environnement PG*)
# ---------------------------------------------------------------------------
pg() {
  PGCONNECT_TIMEOUT="${PSQL_CONNECT_TIMEOUT}" PGPASSWORD="${PGPASS}" \
    psql -h "${PGHOST}" -p "${PGPORT}" -U "${PGUSER}" -d "${PGDB}" \
    "$@" 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# API REST Scaleway : infos statiques de l'instance
# Retourne le nom de l'instance (stdout) pour stockage par l'appelant
# ---------------------------------------------------------------------------
collect_api_instance() {
  local instance_id="$1" env="$2"

  local response
  response=$(curl -sf \
    -H "X-Auth-Token: ${SCW_SECRET_KEY}" \
    "${SCW_API_BASE}/${instance_id}" || echo "{}")

  local instance_name
  instance_name=$(echo "$response" | jq -r '.name // "unknown"')

  # Retourne le nom pour stockage dans le tableau associatif
  echo "$instance_name"

  local labels="env=\"${env}\",instance_id=\"${instance_id}\",instance_name=\"${instance_name}\",region=\"${SCW_REGION}\""

  # Statut de l'instance (1 = ready, 0 = autre)
  local status
  status=$(echo "$response" | jq -r '.status // "unknown"')
  local status_val=0
  [[ "$status" == "ready" ]] && status_val=1
  add_metric "scaleway_db_instance_ready" "$labels" "$status_val"

  # Taille du volume alloué en octets
  local vol_size
  vol_size=$(echo "$response" | jq -r '.volume.size // empty')
  [[ -n "$vol_size" && "$vol_size" =~ ^[0-9]+$ ]] && \
    add_metric "scaleway_db_instance_volume_size_bytes" "$labels" "$vol_size"

  # Haute disponibilité (1 = oui, 0 = non)
  local ha
  ha=$(echo "$response" | jq -r '.is_ha_cluster // false')
  local ha_val=0
  [[ "$ha" == "true" ]] && ha_val=1
  add_metric "scaleway_db_instance_ha" "$labels" "$ha_val"

  # max_connections depuis les settings API (pas besoin de psql)
  local max_conn
  max_conn=$(echo "$response" | jq -r '
    .settings // [] | map(select(.name=="max_connections")) | .[0].value // empty
  ')
  if [[ -n "$max_conn" && "$max_conn" =~ ^[0-9]+$ ]]; then
    add_metric "scaleway_db_instance_max_connections" \
      "env=\"${env}\",instance_id=\"${instance_id}\",instance_name=\"${instance_name}\"" \
      "$max_conn"
  fi

  log "    ✓ API name=${instance_name} status=${status} vol=$(( ${vol_size:-0} / 1073741824 ))GB ha=${ha} max_conn=${max_conn:-n/a}"
}

# ---------------------------------------------------------------------------
# MODE AGRÉGÉ : 1 connexion sur postgres → stats globales + top N
# ---------------------------------------------------------------------------
collect_aggregated() {
  local instance_id="$1" env="$2" instance_name="$3"
  local inst_labels="env=\"${env}\",instance_id=\"${instance_id}\",instance_name=\"${instance_name}\""
  log "  [mode agrégé]"

  # Connexions globales (toute l'instance)
  local conn_data
  conn_data=$(pg -t -A -F '|' -c "
    SELECT
      COUNT(*)                                              AS total,
      COUNT(*) FILTER (WHERE state = 'active')             AS active,
      COUNT(*) FILTER (WHERE state = 'idle')               AS idle,
      COUNT(*) FILTER (WHERE wait_event_type = 'Lock')     AS waiting
    FROM pg_stat_activity
    WHERE datname NOT IN ('postgres','rdb','') AND datname IS NOT NULL;
  ")

  local total=0 active=0 idle=0 waiting=0
  if [[ -n "$conn_data" ]]; then
    IFS='|' read -r total active idle waiting <<< "$conn_data"
    [[ "$total"   =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_connections_total"   "$inst_labels" "$total"
    [[ "$active"  =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_connections_active"  "$inst_labels" "$active"
    [[ "$idle"    =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_connections_idle"    "$inst_labels" "$idle"
    [[ "$waiting" =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_connections_waiting" "$inst_labels" "$waiting"
    log "    ✓ connexions total=${total} active=${active} idle=${idle} waiting=${waiting}"
  fi

  # Ratio connexions / max
  local max_conn
  max_conn=$(pg -t -A -c "SELECT setting FROM pg_settings WHERE name='max_connections';")
  if [[ "$max_conn" =~ ^[0-9]+$ && "${total}" =~ ^[0-9]+$ && "$max_conn" -gt 0 ]]; then
    local ratio
    ratio=$(LC_NUMERIC=C awk "BEGIN { printf \"%.4f\", ${total} / ${max_conn} }")
    add_metric "scaleway_db_pg_connections_ratio" "$inst_labels" "$ratio"
  fi

  # Top N bases par taille
  local size_rows
  size_rows=$(pg -t -A -F '|' -c "
    SELECT datname, pg_database_size(datname)
    FROM pg_database
    WHERE datistemplate = false
      AND datname NOT IN ('postgres','rdb','banner')
      AND datname NOT LIKE 'db-00000000%'
    ORDER BY pg_database_size(datname) DESC
    LIMIT ${DB_TOP_N};
  ")
  if [[ -n "$size_rows" ]]; then
    while IFS='|' read -r dbname size; do
      [[ -z "$dbname" || -z "$size" ]] && continue
      [[ "$size" =~ ^[0-9]+$ ]] && \
        add_metric "scaleway_db_pg_size_bytes" \
          "env=\"${env}\",instance_id=\"${instance_id}\",instance_name=\"${instance_name}\",db=\"${dbname}\"" \
          "$size"
    done <<< "$size_rows"
    log "    ✓ top ${DB_TOP_N} tailles collectées"
  fi

  # Top N bases par connexions actives
  local conn_rows
  conn_rows=$(pg -t -A -F '|' -c "
    SELECT datname, COUNT(*)
    FROM pg_stat_activity
    WHERE datname NOT IN ('postgres','rdb','') AND datname IS NOT NULL
    GROUP BY datname
    ORDER BY COUNT(*) DESC
    LIMIT ${DB_TOP_N};
  ")
  if [[ -n "$conn_rows" ]]; then
    while IFS='|' read -r dbname nb; do
      [[ -z "$dbname" || -z "$nb" ]] && continue
      [[ "$nb" =~ ^[0-9]+$ ]] && \
        add_metric "scaleway_db_pg_connections_per_db" \
          "env=\"${env}\",instance_id=\"${instance_id}\",instance_name=\"${instance_name}\",db=\"${dbname}\"" \
          "$nb"
    done <<< "$conn_rows"
    log "    ✓ top ${DB_TOP_N} connexions par base collectées"
  fi
}

# ---------------------------------------------------------------------------
# MODE DÉTAILLÉ : stats complètes + pg_stat_statements par base
# ---------------------------------------------------------------------------
collect_detailed() {
  local instance_id="$1" env="$2" db="$3" instance_name="$4"
  local labels="env=\"${env}\",instance_id=\"${instance_id}\",instance_name=\"${instance_name}\",db=\"${db}\""

  # Taille
  local db_size
  db_size=$(pg -t -A -c "SELECT pg_database_size(current_database());")
  [[ "$db_size" =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_size_bytes" "$labels" "$db_size"

  # Connexions
  local conn_data
  conn_data=$(pg -t -A -F '|' -c "
    SELECT
      COUNT(*)                                              AS total,
      COUNT(*) FILTER (WHERE state = 'active')             AS active,
      COUNT(*) FILTER (WHERE state = 'idle')               AS idle,
      COUNT(*) FILTER (WHERE wait_event_type = 'Lock')     AS waiting,
      (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') AS max_conn
    FROM pg_stat_activity WHERE datname = current_database();
  ")
  if [[ -n "$conn_data" ]]; then
    IFS='|' read -r total active idle waiting max_conn <<< "$conn_data"
    [[ "$total"    =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_connections_total"   "$labels" "$total"
    [[ "$active"   =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_connections_active"  "$labels" "$active"
    [[ "$idle"     =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_connections_idle"    "$labels" "$idle"
    [[ "$waiting"  =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_connections_waiting" "$labels" "$waiting"
    [[ "$max_conn" =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_max_connections"     "$labels" "$max_conn"
    if [[ "$max_conn" =~ ^[0-9]+$ && "$total" =~ ^[0-9]+$ && "$max_conn" -gt 0 ]]; then
      local ratio
      ratio=$(LC_NUMERIC=C awk "BEGIN { printf \"%.4f\", ${total} / ${max_conn} }")
      add_metric "scaleway_db_pg_connections_ratio" "$labels" "$ratio"
    fi
  fi

  # pg_stat_statements — top 10 requêtes les plus lentes
  local ext_ok
  ext_ok=$(pg -t -A -c "SELECT COUNT(*) FROM pg_extension WHERE extname='pg_stat_statements';")
  if [[ "${ext_ok:-0}" == "1" ]]; then
    local stmts
    stmts=$(pg -t -A -F '§' -c "
      SELECT
        queryid,
        calls,
        ROUND((total_exec_time / NULLIF(calls,0))::numeric, 3),
        ROUND(total_exec_time::numeric, 3),
        ROUND(rows::numeric / NULLIF(calls,0), 2)
      FROM pg_stat_statements
      WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
      ORDER BY total_exec_time DESC
      LIMIT 10;
    ")
    if [[ -n "$stmts" ]]; then
      local rank=1
      while IFS='§' read -r queryid calls avg_ms total_ms avg_rows; do
        [[ -z "$queryid" ]] && continue
        local sl="${labels},queryid=\"${queryid}\",rank=\"${rank}\""
        [[ "$calls"    =~ ^[0-9]+$            ]] && add_metric "scaleway_db_pg_stmt_calls_total"   "$sl" "$calls"
        [[ "$avg_ms"   =~ ^[0-9]+(\.[0-9]+)?$ ]] && add_metric "scaleway_db_pg_stmt_avg_exec_ms"   "$sl" "$avg_ms"
        [[ "$total_ms" =~ ^[0-9]+(\.[0-9]+)?$ ]] && add_metric "scaleway_db_pg_stmt_total_exec_ms" "$sl" "$total_ms"
        [[ "$avg_rows" =~ ^[0-9]+(\.[0-9]+)?$ ]] && add_metric "scaleway_db_pg_stmt_avg_rows"      "$sl" "$avg_rows"
        (( rank++ ))
      done <<< "$stmts"
      log "      ✓ pg_stat_statements top 10"
    fi
  fi
}

# ---------------------------------------------------------------------------
# En-têtes HELP/TYPE
# ---------------------------------------------------------------------------
write_headers() {
  metric_header "scaleway_db_instance_ready"               "gauge" "1 if instance status is ready, 0 otherwise"
  metric_header "scaleway_db_instance_volume_size_bytes"   "gauge" "Allocated volume size in bytes"
  metric_header "scaleway_db_instance_ha"                  "gauge" "1 if instance is a HA cluster"
  metric_header "scaleway_db_instance_max_connections"     "gauge" "max_connections from Scaleway API settings"
  metric_header "scaleway_db_instance_db_count"            "gauge" "Number of user databases on the instance"
  metric_header "scaleway_db_instance_mode"                "gauge" "Collection mode: 0=aggregated 1=detailed -1=unreachable"
  metric_header "scaleway_db_pg_size_bytes"                "gauge" "Size of the PostgreSQL database in bytes"
  metric_header "scaleway_db_pg_connections_total"         "gauge" "Total connections (instance-level in agg mode, db-level in detailed mode)"
  metric_header "scaleway_db_pg_connections_active"        "gauge" "Active (running) connections"
  metric_header "scaleway_db_pg_connections_idle"          "gauge" "Idle connections"
  metric_header "scaleway_db_pg_connections_waiting"       "gauge" "Connections waiting on a lock"
  metric_header "scaleway_db_pg_connections_per_db"        "gauge" "Connections per database (aggregated mode, top N)"
  metric_header "scaleway_db_pg_max_connections"           "gauge" "max_connections PostgreSQL setting (detailed mode)"
  metric_header "scaleway_db_pg_connections_ratio"         "gauge" "Ratio used connections / max_connections (0-1)"
  metric_header "scaleway_db_pg_stmt_calls_total"          "gauge" "Total call count for a tracked statement (detailed mode)"
  metric_header "scaleway_db_pg_stmt_avg_exec_ms"          "gauge" "Average execution time in ms (detailed mode)"
  metric_header "scaleway_db_pg_stmt_total_exec_ms"        "gauge" "Total execution time in ms (detailed mode)"
  metric_header "scaleway_db_pg_stmt_avg_rows"             "gauge" "Average rows returned per call (detailed mode)"
  metric_header "scaleway_db_last_scrape_timestamp"        "gauge" "Unix timestamp of the last successful scrape"
  metric_header "scaleway_db_last_scrape_duration_seconds" "gauge" "Duration of the last scrape in seconds"
}

# ---------------------------------------------------------------------------
# Push vers le Pushgateway
# ---------------------------------------------------------------------------
push_to_gateway() {
  local url="${PUSHGATEWAY_URL}/metrics/job/${PUSHGATEWAY_JOB}"
  local http_code
  http_code=$(printf '%s' "$BUFFER" | curl -sf -o /dev/null -w "%{http_code}" \
    --data-binary @- -X PUT "$url" || echo "000")
  if [[ "$http_code" =~ ^2 ]]; then
    log "  ✓ Push OK (HTTP $http_code)"
  else
    err "  Push ÉCHOUÉ (HTTP $http_code) → $url"
    err "  → kubectl port-forward svc/<pushgateway-svc> 9091:9091 -n grafana"
  fi
}

# ---------------------------------------------------------------------------
# Collecte principale
# ---------------------------------------------------------------------------
do_scrape() {
  local t_start; t_start=$(date +%s)
  BUFFER=""
  write_headers

  # Tableau associatif : instance_id → instance_name
  declare -A INSTANCE_NAMES

  local instance_count
  instance_count=$(echo "$DB_INSTANCES" | jq 'length')
  log "=== Scrape démarré — ${instance_count} instance(s) ==="

  for i in $(seq 0 $(( instance_count - 1 ))); do
    local inst id env host port user pass
    inst=$(echo "$DB_INSTANCES" | jq -r ".[$i]")
    id=$(echo   "$inst" | jq -r '.id')
    env=$(echo  "$inst" | jq -r '.env')
    host=$(echo "$inst" | jq -r '.host')
    port=$(echo "$inst" | jq -r '.port')
    user=$(echo "$inst" | jq -r '.user')
    pass=$(echo "$inst" | jq -r '.pass')

    log ""
    log "--- ${env} / ${id} (${host}:${port}) ---"

    # 1. Infos statiques via API REST + récupération du nom
    local instance_name
    instance_name=$(collect_api_instance "$id" "$env")
    INSTANCE_NAMES["$id"]="$instance_name"

    local inst_labels="env=\"${env}\",instance_id=\"${id}\",instance_name=\"${instance_name}\""

    # 2. Test connectivité psql + listing des bases
    export PGHOST="$host" PGPORT="$port" PGUSER="$user" PGPASS="$pass" PGDB="postgres"

    local db_list
    db_list=$(PGCONNECT_TIMEOUT="${PSQL_CONNECT_TIMEOUT}" PGPASSWORD="$pass" \
      psql -h "$host" -p "$port" -U "$user" -d "postgres" -t -A \
      -c "SELECT datname FROM pg_database
          WHERE datistemplate = false
            AND datname NOT IN ('postgres','rdb','banner')
            AND datname NOT LIKE 'db-00000000%'
          ORDER BY datname;" \
      2>/dev/null || echo "UNREACHABLE")

    if [[ "$db_list" == "UNREACHABLE" || -z "$db_list" ]]; then
      warn "  psql injoignable — skip métriques PG (infos API conservées)"
      add_metric "scaleway_db_instance_mode" "$inst_labels" "-1"
      continue
    fi

    local db_count
    db_count=$(echo "$db_list" | wc -l | tr -d ' ')
    add_metric "scaleway_db_instance_db_count" "$inst_labels" "$db_count"
    log "  → ${db_count} base(s) utilisateur"

    # 3. Mode agrégé ou détaillé
    if (( db_count >= DB_AGGREGATE_THRESHOLD )); then
      add_metric "scaleway_db_instance_mode" "$inst_labels" "0"
      export PGDB="postgres"
      collect_aggregated "$id" "$env" "$instance_name"
    else
      add_metric "scaleway_db_instance_mode" "$inst_labels" "1"
      while IFS= read -r db; do
        [[ -z "$db" ]] && continue
        log "    [db] $db"
        export PGDB="$db"
        collect_detailed "$id" "$env" "$db" "$instance_name"
      done <<< "$db_list"
    fi
  done

  local t_end duration
  t_end=$(date +%s)
  duration=$(( t_end - t_start ))
  BUFFER+="scaleway_db_last_scrape_timestamp ${t_end}"$'\n'
  BUFFER+="scaleway_db_last_scrape_duration_seconds ${duration}"$'\n'

  log ""
  push_to_gateway
  log "=== Terminé en ${duration}s ==="
}

# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------
trap 'log "Arrêt."' EXIT INT TERM

log "Pushgateway          : ${PUSHGATEWAY_URL}"
log "Job                  : ${PUSHGATEWAY_JOB}"
log "Intervalle           : ${SCRAPE_INTERVAL}s"
log "Seuil mode agrégé    : ${DB_AGGREGATE_THRESHOLD} bases"
log "Top N (mode agrégé)  : ${DB_TOP_N}"
log "Timeout connexion    : ${PSQL_CONNECT_TIMEOUT}s"
log ""

while true; do
  do_scrape
  log "Prochain scrape dans ${SCRAPE_INTERVAL}s..."
  sleep "$SCRAPE_INTERVAL"
done