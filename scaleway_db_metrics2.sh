#!/usr/bin/env bash
# =============================================================================
# scaleway_db_metrics.sh
#
# Collecte toutes les N secondes les métriques des bases PostgreSQL Scaleway
# et les pousse vers un Prometheus Pushgateway.
#
# Sources de données :
#   - API REST Scaleway  → infos statiques par instance (volume, status, max_conn, HA)
#   - psql direct        → métriques live (connexions, tailles, requêtes lentes)
#   - Cockpit Scaleway   → métriques infra (CPU, RAM, disk I/O, replication lag)
#
# Deux modes selon le nombre de bases sur l'instance :
#   MODE AGRÉGÉ   (>= DB_AGGREGATE_THRESHOLD)  → 1 connexion/instance, top N tailles
#   MODE DÉTAILLÉ (< DB_AGGREGATE_THRESHOLD)   → 1 connexion/base, pg_stat_statements
#
# Instances sur réseau privé (172.x) : skippées en psql si injoignables,
# mais les infos API REST et Cockpit sont quand même collectées.
#
# Prérequis : curl, jq, psql
#
# Usage :
#   chmod +x scaleway_db_metrics.sh && ./scaleway_db_metrics.sh
#
# Port-forward Pushgateway :
#   kubectl port-forward svc/<pushgateway-svc> 9091:9091 -n grafana
# ==============================================================================

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
COCKPIT_QUERY_TIMEOUT="${COCKPIT_QUERY_TIMEOUT:-10}"

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
declare -A HEADERS_WRITTEN=()

metric_header() {
  local name="$1" type="$2" help="$3"
  # N'écrit l'en-tête qu'une seule fois par nom de métrique
  if [[ -z "${HEADERS_WRITTEN[$name]:-}" ]]; then
    BUFFER+="# HELP ${name} ${help}"$'\n'"# TYPE ${name} ${type}"$'\n'
    HEADERS_WRITTEN[$name]=1
  fi
}

add_metric() {
  local name="$1" value="$2" labels="$3"
  BUFFER+="${name}{${labels}} ${value}"$'\n'
}

# ---------------------------------------------------------------------------
# Wrapper psql (utilise les variables PGHOST/PGPORT/PGUSER/PGPASS/PGDB)
# ---------------------------------------------------------------------------
pg() {
  PGCONNECT_TIMEOUT="${PSQL_CONNECT_TIMEOUT}" PGPASSWORD="${PGPASS}" \
    psql -h "${PGHOST}" -p "${PGPORT}" -U "${PGUSER}" -d "${PGDB}" \
    "$@" 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# API REST Scaleway : infos statiques de l'instance
# ---------------------------------------------------------------------------
collect_api_instance() {
  local instance_id="$1" env="$2"
  local labels="env=\"${env}\",instance_id=\"${instance_id}\",region=\"${SCW_REGION}\""

  local response
  response=$(curl -sf \
    -H "X-Auth-Token: ${SCW_SECRET_KEY}" \
    "${SCW_API_BASE}/${instance_id}" || echo "{}")

  # Statut de l'instance (1 = ready, 0 = autre)
  local status
  status=$(echo "$response" | jq -r '.status // "unknown"')
  local status_val=0
  [[ "$status" == "ready" ]] && status_val=1
  add_metric "scaleway_db_instance_ready" "$status_val" "$labels"

  # Taille du volume alloué en octets
  local vol_size
  vol_size=$(echo "$response" | jq -r '.volume.size // empty')
  [[ -n "$vol_size" && "$vol_size" =~ ^[0-9]+$ ]] && \
    add_metric "scaleway_db_instance_volume_size_bytes" "$vol_size" "$labels"

  # Haute disponibilité (1 = oui, 0 = non)
  local ha
  ha=$(echo "$response" | jq -r '.is_ha_cluster // false')
  [[ "$ha" == "true" ]] && add_metric "scaleway_db_instance_ha" "1" "$labels" \
                        || add_metric "scaleway_db_instance_ha" "0" "$labels"

  # max_connections depuis les settings API (pas besoin de psql)
  local max_conn
  max_conn=$(echo "$response" | jq -r '
    .settings // [] | map(select(.name=="max_connections")) | .[0].value // empty
  ')
  [[ -n "$max_conn" && "$max_conn" =~ ^[0-9]+$ ]] && \
    add_metric "scaleway_db_instance_max_connections" "$max_conn" \
      "env=\"${env}\",instance_id=\"${instance_id}\""

  log "    ✓ API status=$status vol=$(( ${vol_size:-0} / 1073741824 ))GB ha=$ha max_conn=${max_conn:-n/a}"
}

# ---------------------------------------------------------------------------
# Cockpit Scaleway : métriques infra de l'instance
#
# IMPORTANT : Les noms de métriques exposées par Scaleway dans Cockpit/Mimir
# pour les instances RDB managées doivent être vérifiés dans votre Cockpit.
# Pour les découvrir : curl -H "Authorization: Bearer <token>" \
#   "<cockpit_url>/prometheus/api/v1/label/__name__/values" | jq '.data[]' | grep -i rdb
#
# Les noms ci-dessous sont les noms les plus courants observés sur Scaleway RDB.
# Adaptez-les si vos métriques ont un préfixe différent (ex: postgresql_*).
# ---------------------------------------------------------------------------
collect_cockpit_metrics() {
  local instance_id="$1" env="$2" cockpit_url="$3" cockpit_token="$4"
  local labels="env=\"${env}\",instance_id=\"${instance_id}\""

  # Format : "nom_cockpit|nom_pushgateway|type|description"
  # Listez ici les métriques réellement disponibles dans votre Cockpit.
  # Pour les découvrir : voir commentaire ci-dessus.
  local -a COCKPIT_METRICS=(
    "rdb_cpu_usage_percent|scaleway_cockpit_rdb_cpu_usage_percent|gauge|CPU usage percent of the database instance"
    "rdb_mem_usage_percent|scaleway_cockpit_rdb_mem_usage_percent|gauge|Memory usage percent of the database instance"
    "rdb_disk_usage_percent|scaleway_cockpit_rdb_disk_usage_percent|gauge|Disk usage percent of the database instance"
    "rdb_disk_iops_read|scaleway_cockpit_rdb_disk_iops_read|gauge|Disk read IOPS of the database instance"
    "rdb_disk_iops_write|scaleway_cockpit_rdb_disk_iops_write|gauge|Disk write IOPS of the database instance"
    "rdb_disk_throughput_read|scaleway_cockpit_rdb_disk_throughput_read_bytes|gauge|Disk read throughput in bytes per second"
    "rdb_disk_throughput_write|scaleway_cockpit_rdb_disk_throughput_write_bytes|gauge|Disk write throughput in bytes per second"
    "rdb_net_rx|scaleway_cockpit_rdb_net_rx_bytes|gauge|Network received bytes per second"
    "rdb_net_tx|scaleway_cockpit_rdb_net_tx_bytes|gauge|Network transmitted bytes per second"
    "rdb_active_connections|scaleway_cockpit_rdb_active_connections|gauge|Number of active connections reported by Cockpit"
    "rdb_replication_lag|scaleway_cockpit_rdb_replication_lag_seconds|gauge|Replication lag in seconds (HA / read replicas)"
  )

  # Label de filtrage — Scaleway utilise resource_id OU instance_id selon la version du Cockpit.
  # Essayez les deux si aucune métrique ne remonte.
  local filter_label="resource_id"
  local query_base="${cockpit_url}/prometheus/api/v1/query"
  local success_count=0

  for entry in "${COCKPIT_METRICS[@]}"; do
    IFS='|' read -r cockpit_name prom_name prom_type description <<< "$entry"

    local query="${cockpit_name}{${filter_label}=\"${instance_id}\"}"
    local response
    response=$(curl -sf \
      --max-time "${COCKPIT_QUERY_TIMEOUT}" \
      -H "Authorization: Bearer ${cockpit_token}" \
      -G "${query_base}" \
      --data-urlencode "query=${query}" \
      2>/dev/null || echo "{}")

    local value
    value=$(echo "$response" | jq -r '.data.result[0].value[1] // empty' 2>/dev/null || true)

    if [[ -n "$value" && "$value" != "NaN" && "$value" =~ ^-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$ ]]; then
      metric_header "${prom_name}" "${prom_type}" "${description}"
      add_metric "${prom_name}" "${value}" "${labels}"
      success_count=$(( success_count + 1 ))
    fi
  done

  if (( success_count > 0 )); then
    log "    ✓ Cockpit ${success_count}/${#COCKPIT_METRICS[@]} métriques collectées"
  else
    warn "    Cockpit : aucune métrique pour ${instance_id}."
    warn "    → Vérifiez cockpit_url, cockpit_token (rôle Query requis), et les noms de métriques."
    warn "    → Pour lister les métriques disponibles :"
    warn "       curl -H 'Authorization: Bearer <token>' '${cockpit_url}/prometheus/api/v1/label/__name__/values'"
  fi
}

# ---------------------------------------------------------------------------
# MODE AGRÉGÉ : 1 connexion sur postgres → stats globales + top N
# ---------------------------------------------------------------------------
collect_aggregated() {
  local instance_id="$1" env="$2"
  local inst_labels="env=\"${env}\",instance_id=\"${instance_id}\""
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
    [[ "$total"   =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_connections_total"   "$total"   "$inst_labels"
    [[ "$active"  =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_connections_active"  "$active"  "$inst_labels"
    [[ "$idle"    =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_connections_idle"    "$idle"    "$inst_labels"
    [[ "$waiting" =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_connections_waiting" "$waiting" "$inst_labels"
    log "    ✓ connexions total=$total active=$active idle=$idle waiting=$waiting"
  fi

  # Ratio connexions / max
  local max_conn
  max_conn=$(pg -t -A -c "SELECT setting FROM pg_settings WHERE name='max_connections';")
  if [[ "$max_conn" =~ ^[0-9]+$ && "$total" =~ ^[0-9]+$ && "$max_conn" -gt 0 ]]; then
    local ratio
    ratio=$(awk "BEGIN { printf \"%.4f\", ${total} / ${max_conn} }")
    add_metric "scaleway_db_pg_connections_ratio"       "$ratio"    "$inst_labels"
    add_metric "scaleway_db_pg_max_connections"         "$max_conn" "$inst_labels"
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
        add_metric "scaleway_db_pg_size_bytes" "$size" \
          "env=\"${env}\",instance_id=\"${instance_id}\",db=\"${dbname}\""
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
        add_metric "scaleway_db_pg_connections_per_db" "$nb" \
          "env=\"${env}\",instance_id=\"${instance_id}\",db=\"${dbname}\""
    done <<< "$conn_rows"
    log "    ✓ top ${DB_TOP_N} connexions par base collectées"
  fi

  # ------------------------------------------------------------------
  # Requêtes lentes en mode agrégé : on interroge pg_stat_statements
  # sur toutes les BDD, groupé par queryid (sans connexion par BDD).
  # On expose le top N par total_exec_time au niveau instance.
  # ------------------------------------------------------------------
  local ext_ok
  ext_ok=$(pg -t -A -c "SELECT COUNT(*) FROM pg_extension WHERE extname='pg_stat_statements';")
  if [[ "${ext_ok:-0}" == "1" ]]; then
    local stmts
    stmts=$(pg -t -A -F '§' -c "
      SELECT
        queryid,
        calls,
        ROUND((total_exec_time / NULLIF(calls,0))::numeric, 3)  AS avg_ms,
        ROUND(total_exec_time::numeric, 3)                       AS total_ms,
        ROUND(max_exec_time::numeric, 3)                         AS max_ms,
        ROUND(rows::numeric / NULLIF(calls,0), 2)                AS avg_rows
      FROM pg_stat_statements
      ORDER BY total_exec_time DESC
      LIMIT ${DB_TOP_N};
    ")
    if [[ -n "$stmts" ]]; then
      local rank=1
      while IFS='§' read -r queryid calls avg_ms total_ms max_ms avg_rows; do
        [[ -z "$queryid" ]] && continue
        local sl="${inst_labels},queryid=\"${queryid}\",rank=\"${rank}\""
        [[ "$calls"    =~ ^[0-9]+$             ]] && add_metric "scaleway_db_pg_stmt_calls_total"   "$calls"    "$sl"
        [[ "$avg_ms"   =~ ^[0-9]+(\.[0-9]+)?$  ]] && add_metric "scaleway_db_pg_stmt_avg_exec_ms"   "$avg_ms"   "$sl"
        [[ "$total_ms" =~ ^[0-9]+(\.[0-9]+)?$  ]] && add_metric "scaleway_db_pg_stmt_total_exec_ms" "$total_ms" "$sl"
        [[ "$max_ms"   =~ ^[0-9]+(\.[0-9]+)?$  ]] && add_metric "scaleway_db_pg_stmt_max_exec_ms"   "$max_ms"   "$sl"
        [[ "$avg_rows" =~ ^[0-9]+(\.[0-9]+)?$  ]] && add_metric "scaleway_db_pg_stmt_avg_rows"      "$avg_rows" "$sl"
        rank=$(( rank + 1 ))
      done <<< "$stmts"
      log "    ✓ pg_stat_statements top ${DB_TOP_N} (mode agrégé)"
    fi
  else
    warn "    pg_stat_statements non installée sur cette instance"
  fi
}

# ---------------------------------------------------------------------------
# MODE DÉTAILLÉ : stats complètes + pg_stat_statements par base
# ---------------------------------------------------------------------------
collect_detailed() {
  local instance_id="$1" env="$2" db="$3"
  local labels="env=\"${env}\",instance_id=\"${instance_id}\",db=\"${db}\""

  # Taille
  local db_size
  db_size=$(pg -t -A -c "SELECT pg_database_size(current_database());")
  [[ "$db_size" =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_size_bytes" "$db_size" "$labels"

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
    [[ "$total"    =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_connections_total"   "$total"    "$labels"
    [[ "$active"   =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_connections_active"  "$active"   "$labels"
    [[ "$idle"     =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_connections_idle"    "$idle"     "$labels"
    [[ "$waiting"  =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_connections_waiting" "$waiting"  "$labels"
    [[ "$max_conn" =~ ^[0-9]+$ ]] && add_metric "scaleway_db_pg_max_connections"     "$max_conn" "$labels"
    if [[ "$max_conn" =~ ^[0-9]+$ && "$total" =~ ^[0-9]+$ && "$max_conn" -gt 0 ]]; then
      local ratio
      ratio=$(awk "BEGIN { printf \"%.4f\", $total / $max_conn }")
      add_metric "scaleway_db_pg_connections_ratio" "$ratio" "$labels"
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
        ROUND((total_exec_time / NULLIF(calls,0))::numeric, 3)  AS avg_ms,
        ROUND(total_exec_time::numeric, 3)                       AS total_ms,
        ROUND(max_exec_time::numeric, 3)                         AS max_ms,
        ROUND(rows::numeric / NULLIF(calls,0), 2)                AS avg_rows
      FROM pg_stat_statements
      WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
      ORDER BY total_exec_time DESC
      LIMIT 10;
    ")
    if [[ -n "$stmts" ]]; then
      local rank=1
      while IFS='§' read -r queryid calls avg_ms total_ms max_ms avg_rows; do
        [[ -z "$queryid" ]] && continue
        local sl="${labels},queryid=\"${queryid}\",rank=\"${rank}\""
        [[ "$calls"    =~ ^[0-9]+$            ]] && add_metric "scaleway_db_pg_stmt_calls_total"   "$calls"    "$sl"
        [[ "$avg_ms"   =~ ^[0-9]+(\.[0-9]+)?$ ]] && add_metric "scaleway_db_pg_stmt_avg_exec_ms"   "$avg_ms"   "$sl"
        [[ "$total_ms" =~ ^[0-9]+(\.[0-9]+)?$ ]] && add_metric "scaleway_db_pg_stmt_total_exec_ms" "$total_ms" "$sl"
        [[ "$max_ms"   =~ ^[0-9]+(\.[0-9]+)?$ ]] && add_metric "scaleway_db_pg_stmt_max_exec_ms"   "$max_ms"   "$sl"
        [[ "$avg_rows" =~ ^[0-9]+(\.[0-9]+)?$ ]] && add_metric "scaleway_db_pg_stmt_avg_rows"      "$avg_rows" "$sl"
        rank=$(( rank + 1 ))
      done <<< "$stmts"
      log "      ✓ pg_stat_statements top 10"
    fi
  else
    warn "      pg_stat_statements non installée sur ${db}"
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
  metric_header "scaleway_db_pg_max_connections"           "gauge" "max_connections PostgreSQL setting"
  metric_header "scaleway_db_pg_connections_ratio"         "gauge" "Ratio used connections / max_connections (0-1)"
  metric_header "scaleway_db_pg_stmt_calls_total"          "gauge" "Total call count for a tracked statement"
  metric_header "scaleway_db_pg_stmt_avg_exec_ms"          "gauge" "Average execution time in ms"
  metric_header "scaleway_db_pg_stmt_total_exec_ms"        "gauge" "Total execution time in ms"
  metric_header "scaleway_db_pg_stmt_max_exec_ms"          "gauge" "Max (worst) execution time ever recorded in ms"
  metric_header "scaleway_db_pg_stmt_avg_rows"             "gauge" "Average rows returned per call"
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
  declare -gA HEADERS_WRITTEN=()   # reset pour chaque scrape
  write_headers

  local instance_count
  instance_count=$(echo "$DB_INSTANCES" | jq 'length')
  log "=== Scrape démarré — $instance_count instances ==="

  for i in $(seq 0 $(( instance_count - 1 ))); do
    local inst id env host port user pass cockpit_token cockpit_url
    inst=$(echo "$DB_INSTANCES" | jq -r ".[$i]")
    id=$(echo            "$inst" | jq -r '.id')
    env=$(echo           "$inst" | jq -r '.env')
    host=$(echo          "$inst" | jq -r '.host')
    port=$(echo          "$inst" | jq -r '.port')
    user=$(echo          "$inst" | jq -r '.user')
    pass=$(echo          "$inst" | jq -r '.pass')
    cockpit_token=$(echo "$inst" | jq -r '.cockpit_token // empty')
    cockpit_url=$(echo   "$inst" | jq -r '.cockpit_url   // empty')

    log ""
    log "--- $env / $id ($host:$port) ---"

    # 1. Infos statiques via API REST
    collect_api_instance "$id" "$env"

    # 2. Métriques infra via Cockpit (CPU, RAM, disk, réseau, replication lag)
    if [[ -n "$cockpit_token" && -n "$cockpit_url" ]]; then
      collect_cockpit_metrics "$id" "$env" "$cockpit_url" "$cockpit_token"
    else
      warn "  Cockpit non configuré pour cette instance (cockpit_token / cockpit_url manquants)"
    fi

    # 3. Test connectivité psql + listing des bases
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

    local inst_labels="env=\"${env}\",instance_id=\"${id}\""

    if [[ "$db_list" == "UNREACHABLE" || -z "$db_list" ]]; then
      warn "  psql injoignable — skip métriques PG (infos API + Cockpit conservées)"
      add_metric "scaleway_db_instance_mode" "-1" "$inst_labels"
      continue
    fi

    local db_count
    db_count=$(echo "$db_list" | wc -l | tr -d ' ')
    add_metric "scaleway_db_instance_db_count" "$db_count" "$inst_labels"
    log "  → $db_count base(s) utilisateur"

    # 4. Mode agrégé ou détaillé
    if (( db_count >= DB_AGGREGATE_THRESHOLD )); then
      add_metric "scaleway_db_instance_mode" "0" "$inst_labels"
      export PGDB="postgres"
      collect_aggregated "$id" "$env"
    else
      add_metric "scaleway_db_instance_mode" "1" "$inst_labels"
      while IFS= read -r db; do
        [[ -z "$db" ]] && continue
        log "    [db] $db"
        export PGDB="$db"
        collect_detailed "$id" "$env" "$db"
      done <<< "$db_list"
    fi
  done

  local t_end duration
  t_end=$(date +%s); duration=$(( t_end - t_start ))
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
log "Timeout Cockpit      : ${COCKPIT_QUERY_TIMEOUT}s"
log ""

while true; do
  do_scrape
  log "Prochain scrape dans ${SCRAPE_INTERVAL}s..."
  sleep "$SCRAPE_INTERVAL"
done