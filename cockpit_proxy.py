#!/usr/bin/env python3
"""
monitoring_proxy.py
===================
Agrège les métriques de :
  1. Scaleway Cockpit (via /federate)      -> préfixe "cockpit_"
  2. PMM Prometheus (node_exporter + postgres_exporter) -> préfixe "monitoring_"

Pousse tout vers un Pushgateway Prometheus. Au lieu d'un label
`environment`, chaque métrique est préfixée directement dans son nom :
  - cockpit_cpu_seconds_total{...}      (Scaleway)
  - monitoring_cpu_seconds_total{...}   (local / PMM)

Expose aussi :
  GET /health    → état global
  GET /metrics   → métriques internes du proxy

Configuration via variables d'environnement :
  COCKPIT_PROJECTS        JSON : liste de projets Scaleway
  PUSHGATEWAY_URL         URL du Pushgateway
  PUSH_INTERVAL           Intervalle de push en secondes (défaut : 60)
  PMM_PROMETHEUS_URL      URL du Prometheus de PMM
                          (défaut : http://monitoring-service.grafana.svc.cluster.local/prometheus)
  PMM_BASIC_AUTH_USER     Utilisateur pour PMM (défaut : admin)
  PMM_BASIC_AUTH_PASSWORD Mot de passe PMM (défaut : "")
  PROXY_PORT              Port d'écoute (défaut : 8000)
  SCRAPE_TIMEOUT          Timeout par scrape en secondes (défaut : 30)
  LOG_LEVEL               DEBUG | INFO | WARNING (défaut : INFO)
  COCKPIT_METRIC_PREFIX   Préfixe pour les métriques Scaleway (défaut : "cockpit_")
  PMM_METRIC_PREFIX       Préfixe pour les métriques PMM/local (défaut : "monitoring_")

NOTE IMPORTANTE (fix) :
  PMM (VictoriaMetrics) refuse une requête /federate du type {__name__=~".+"}
  dès que le serveur dépasse ~1 000 000 de timeseries ("the number of matching
  timeseries exceeds 1000000"). On filtre donc désormais DIRECTEMENT par job
  (node_exporter_*, postgres_exporter_*) dans la requête envoyée à PMM, au lieu
  de demander tout puis filtrer après coup côté proxy.
"""


import json
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlencode
import urllib.request
import urllib.error
import base64


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("cockpit-proxy")

PROXY_PORT      = int(os.environ.get("PROXY_PORT", "8000"))
SCRAPE_TIMEOUT  = int(os.environ.get("SCRAPE_TIMEOUT", "30"))
PUSH_INTERVAL   = int(os.environ.get("PUSH_INTERVAL", "60"))
PUSHGATEWAY_URL = os.environ.get(
    "PUSHGATEWAY_URL",
    "http://prometheus-pushgateway.grafana.svc.cluster.local:9091"
).rstrip("/")

PMM_PROMETHEUS_URL      = os.environ.get(
    "PMM_PROMETHEUS_URL",
    "http://monitoring-service.grafana.svc.cluster.local/prometheus"
).rstrip("/")
PMM_BASIC_AUTH_USER     = os.environ.get("PMM_BASIC_AUTH_USER", "admin")
PMM_BASIC_AUTH_PASSWORD = os.environ.get("PMM_BASIC_AUTH_PASSWORD", "")

# Préfixes appliqués directement sur le NOM des métriques (pas un label)
COCKPIT_METRIC_PREFIX = os.environ.get("COCKPIT_METRIC_PREFIX", "cockpit_")
PMM_METRIC_PREFIX     = os.environ.get("PMM_METRIC_PREFIX", "monitoring_")


# Métriques exclues du scrape (préfixes, appliqués sur les lignes # HELP/TYPE et séries)
METRIC_BLACKLIST_RE = re.compile(
    r'^(?:# (?:HELP|TYPE) )?object_storage_bucket_'
)

PMM_JOBS = [
    "node_exporter_*",
    "postgres_exporter_*",
]

PMM_METRIC_NAMES = [
    "node_cpu_seconds_total",
    "node_memory_MemTotal_bytes",
    "node_memory_MemAvailable_bytes",
    "pg_stat_database_numbackends",
    "pg_settings_max_connections",
]

def load_projects() -> list[dict]:
    raw = os.environ.get("COCKPIT_PROJECTS", "")
    if not raw:
        log.warning("Variable COCKPIT_PROJECTS vide — Scaleway disabled")
        return []
    try:
        projects = json.loads(raw)
    except json.JSONDecodeError as e:
        log.error("COCKPIT_PROJECTS invalide (JSON) : %s", e)
        return []
    for p in projects:
        for field in ("name", "url", "token"):
            if field not in p:
                log.error("Projet mal configuré, champ manquant '%s' : %s", field, p)
                return []
    log.info("Projets Scaleway chargés : %s", [p["name"] for p in projects])
    return projects


COCKPIT_PROJECTS = load_projects()


# État partagé (thread-safe via lock)
_state_lock = threading.Lock()
_state = {
    "last_push": None,
    "last_push_duration": None,
    "last_push_status": "pending",
    "sources_ok": [],
    "sources_fail": [],
}

def scrape_prometheus(url: str, query: str, auth_user: str = "", auth_password: str = "") -> tuple[str | None, float]:
    """
    Scrape un endpoint Prometheus /federate et retourne le texte brut.
    """
    base = url.rstrip("/") + "/federate"
    q = urlencode({"match[]": query})
    full_url = f"{base}?{q}"

    t0 = time.monotonic()
    req = urllib.request.Request(full_url)

    # Ajoute l'auth Basic si fournie
    if auth_user and auth_password:
        credentials = base64.b64encode(f"{auth_user}:{auth_password}".encode()).decode()
        req.add_header("Authorization", f"Basic {credentials}")

    try:
        with urllib.request.urlopen(req, timeout=SCRAPE_TIMEOUT) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        # On lit le corps de l'erreur pour le logguer : VictoriaMetrics/Prometheus
        # renvoie souvent un message précis (ex: dépassement de limite de séries)
        try:
            err_body = e.read().decode("utf-8", errors="replace")[:500]
        except Exception:
            err_body = ""
        log.warning("HTTP %s lors du scrape de %s — %s", e.code, full_url, err_body)
        return None, time.monotonic() - t0
    except Exception as e:
        log.warning("Erreur scrape %s : %s", full_url, e)
        return None, time.monotonic() - t0

    return raw, time.monotonic() - t0

# ---------------------------------------------------------------------------
# Scrape d'un projet Cockpit
# ---------------------------------------------------------------------------

_RE_WITH_LABELS    = re.compile(r'^(\w+)\{([^}]*)\}(.*)$')
_RE_WITHOUT_LABELS = re.compile(r'^(\w+)(\s+.*)$')

# Pour préfixer le NOM de la métrique (et non un label)
_RE_HELP_TYPE_NAME = re.compile(r'^(# (?:HELP|TYPE) )([a-zA-Z_:][a-zA-Z0-9_:]*)(.*)$')
_RE_METRIC_NAME     = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{.*?\})?(\s+.*)$')


def _inject_label(line: str, label_kv: str) -> str:
    """Injecte un label dans une ligne de métrique Prometheus."""
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return line

    m = _RE_WITH_LABELS.match(stripped)
    if m:
        name, existing, rest = m.groups()
        new_labels = f"{existing},{label_kv}" if existing else label_kv
        return f"{name}{{{new_labels}}}{rest}\n"

    m = _RE_WITHOUT_LABELS.match(stripped)
    if m:
        name, rest = m.groups()
        return f"{name}{{{label_kv}}}{rest}\n"

    return line


def _prefix_metric_name(line: str, prefix: str) -> str:
    """
    Préfixe le NOM de la métrique (pas un label).
    Ex: cpu_seconds_total{...} 12  ->  cockpit_cpu_seconds_total{...} 12
    S'applique aussi aux lignes # HELP / # TYPE pour rester cohérent.
    """
    if not prefix:
        return line

    stripped = line.rstrip("\n")
    if not stripped:
        return line

    m = _RE_HELP_TYPE_NAME.match(stripped)
    if m:
        head, name, rest = m.groups()
        return f"{head}{prefix}{name}{rest}\n"

    m = _RE_METRIC_NAME.match(stripped)
    if m:
        name, labels, rest = m.groups()
        labels = labels or ""
        return f"{prefix}{name}{labels}{rest}\n"

    return line


def _deduplicate(lines: list[str]) -> list[str]:
    """
    Déduplique les lignes de métriques.
    Le /federate renvoie plusieurs points dans le temps pour une même série.
    Le Pushgateway n'accepte qu'une seule valeur par série — on garde la dernière.
    """
    help_lines = {}
    type_lines = {}
    metric_lines = {}

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        if stripped.startswith("# HELP "):
            parts = stripped.split(" ", 3)
            if len(parts) >= 3:
                help_lines[parts[2]] = line
        elif stripped.startswith("# TYPE "):
            parts = stripped.split(" ", 3)
            if len(parts) >= 3:
                type_lines[parts[2]] = line
        elif not stripped.startswith("#"):
            if "{" in stripped:
                brace_end = stripped.rindex("}") + 1
                key = stripped[:brace_end]
            else:
                key = stripped.split(" ")[0]
            metric_lines[key] = line

    result = []
    result.extend(help_lines.values())
    result.extend(type_lines.values())
    result.extend(metric_lines.values())
    return result


def scrape_cockpit_project(project: dict) -> tuple[str, str | None, float]:
    """Scrape un projet Scaleway Cockpit. Préfixe toutes les métriques avec COCKPIT_METRIC_PREFIX."""
    name  = project["name"]
    # On garde un label pour distinguer les projets Scaleway entre eux
    label = f'scaleway_project="{name}"'
    base  = project["url"].rstrip("/") + "/federate"
    query = urlencode({"match[]": '{__name__=~".+"}'})
    url   = f"{base}?{query}"
    token = project["token"]

    t0 = time.monotonic()
    req = urllib.request.Request(url, headers={"X-Token": token})
    try:
        with urllib.request.urlopen(req, timeout=SCRAPE_TIMEOUT) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        log.warning("[Cockpit:%s] HTTP %s", name, e.code)
        return name, None, time.monotonic() - t0
    except Exception as e:
        log.warning("[Cockpit:%s] Erreur : %s", name, e)
        return name, None, time.monotonic() - t0

    duration = time.monotonic() - t0

    # Filtrer les métriques object_storage_bucket_*
    lines = raw.splitlines(keepends=True)
    before = len(lines)
    lines = [l for l in lines if not METRIC_BLACKLIST_RE.match(l.lstrip())]
    dropped = before - len(lines)
    if dropped:
        log.debug("[Cockpit:%s] %d lignes object_storage_bucket_* supprimées", name, dropped)

    # 1) Label du projet (pour distinguer plusieurs projets Scaleway)
    labeled = [_inject_label(line, label) for line in lines]
    # 2) Préfixe du nom de la métrique -> cockpit_xxx
    prefixed = [_prefix_metric_name(line, COCKPIT_METRIC_PREFIX) for line in labeled]

    # Dédup
    deduped = _deduplicate(prefixed)
    log.debug(
        "[Cockpit:%s] scrape OK en %.2fs — %d lignes → %d après filtre+dédup",
        name, duration, before, len(deduped)
    )

    # Ajouter les métriques internes (non préfixées : ce sont des métriques du proxy lui-même)
    meta = (
        f"# HELP monitoring_proxy_scrape_success 1 if last scrape succeeded\n"
        f"# TYPE monitoring_proxy_scrape_success gauge\n"
        f"monitoring_proxy_scrape_success{{source=\"cockpit\",{label}}} 1\n"
        f"# HELP monitoring_proxy_scrape_duration_seconds Duration of last scrape\n"
        f"# TYPE monitoring_proxy_scrape_duration_seconds gauge\n"
        f"monitoring_proxy_scrape_duration_seconds{{source=\"cockpit\",{label}}} {duration:.3f}\n"
    )

    return name, "".join(deduped) + meta, duration


def _build_pmm_job_query() -> str:
    """
    Construit un sélecteur Prometheus/VictoriaMetrics restreint aux jobs PMM voulus,
    ex: {job=~"node_exporter_.*|postgres_exporter_.*"}

    IMPORTANT (fix) : VictoriaMetrics (utilisé par PMM) refuse une requête du type
    {__name__=~".+"} sur un serveur avec beaucoup de métriques : erreur
    "the number of matching timeseries exceeds 1000000". Il faut donc filtrer
    par job DIRECTEMENT dans la requête envoyée à /federate, pas seulement
    après coup côté proxy (ce qui ne fonctionne pas si la requête initiale
    est déjà rejetée par le serveur).
    """
    job_patterns = [job.replace("*", ".*") for job in PMM_JOBS]
    job_regex = "|".join(job_patterns)
    name_regex = "|".join(PMM_METRIC_NAMES)
    return f'{{__name__=~"{name_regex}", job=~"{job_regex}"}}'


def scrape_pmm() -> tuple[str, str | None, float]:
    """Scrape PMM Prometheus. Préfixe toutes les métriques avec PMM_METRIC_PREFIX."""
    t0 = time.monotonic()

    # Filtre DIRECTEMENT côté VictoriaMetrics par job (node_exporter / postgres_exporter)
    # au lieu de {__name__=~".+"} qui dépasse la limite de séries de PMM (voir fix ci-dessus).
    query = _build_pmm_job_query()
    raw, duration = scrape_prometheus(
        PMM_PROMETHEUS_URL,
        query,
        auth_user=PMM_BASIC_AUTH_USER,
        auth_password=PMM_BASIC_AUTH_PASSWORD,
    )

    if not raw:
        log.warning("[PMM] Erreur scrape")
        return "pmm", None, time.monotonic() - t0

    # Filtre de sécurité en post-traitement (garde uniquement les jobs attendus,
    # au cas où la regex serveur aurait matché plus large que prévu)
    lines = raw.splitlines(keepends=True)
    before = len(lines)

    filtered_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("#") or not stripped:
            filtered_lines.append(line)
        elif "job=" in stripped and any(job.replace("*", "") in stripped for job in PMM_JOBS):
            filtered_lines.append(line)

    dropped = before - len(filtered_lines)
    if dropped:
        log.debug("[PMM] %d lignes filtrées (jobs non-PMM)", dropped)

    # Préfixe du nom de la métrique -> monitoring_xxx
    prefixed = [_prefix_metric_name(line, PMM_METRIC_PREFIX) for line in filtered_lines]

    # Dédup
    deduped = _deduplicate(prefixed)
    log.debug(
        "[PMM] scrape OK en %.2fs — %d lignes → %d après filtre+dédup",
        duration, before, len(deduped)
    )

    # Meta
    meta = (
        f"# HELP monitoring_proxy_scrape_success 1 if last scrape succeeded\n"
        f"# TYPE monitoring_proxy_scrape_success gauge\n"
        f"monitoring_proxy_scrape_success{{source=\"pmm\"}} 1\n"
        f"# HELP monitoring_proxy_scrape_duration_seconds Duration of last scrape\n"
        f"# TYPE monitoring_proxy_scrape_duration_seconds gauge\n"
        f"monitoring_proxy_scrape_duration_seconds{{source=\"pmm\"}} {duration:.3f}\n"
    )

    return "pmm", "".join(deduped) + meta, duration


# ---------------------------------------------------------------------------
# Push vers le Pushgateway
# ---------------------------------------------------------------------------

def push_to_gateway(job: str, instance: str, data: str) -> bool:
    """Pousse les métriques vers le Pushgateway via DELETE puis PUT."""
    base_url = f"{PUSHGATEWAY_URL}/metrics/job/{job}/instance/{instance}"

    # DELETE
    try:
        del_req = urllib.request.Request(base_url, method="DELETE")
        with urllib.request.urlopen(del_req, timeout=10) as resp:
            log.debug("DELETE OK [%s] : HTTP %s", instance, resp.status)
    except urllib.error.HTTPError as e:
        if e.code != 404:
            log.warning("DELETE [%s] HTTP %s (non bloquant)", instance, e.code)
    except Exception as e:
        log.warning("DELETE [%s] échoué (non bloquant) : %s", instance, e)

    # PUT
    body = data.encode("utf-8")
    req = urllib.request.Request(
        base_url,
        data=body,
        method="PUT",
        headers={"Content-Type": "text/plain; version=0.0.4; charset=utf-8"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            log.debug("Push OK [%s] : HTTP %s", instance, resp.status)
            return True
    except urllib.error.HTTPError as e:
        try:
            err_body = e.read().decode("utf-8", errors="replace")[:300]
        except Exception:
            err_body = ""
        log.error("Erreur push [%s] HTTP %s — %s", instance, e.code, err_body)
        return False


# ---------------------------------------------------------------------------
# Boucle principale
# ---------------------------------------------------------------------------

def scrape_and_push():
    """Scrape Scaleway (cockpit_*) + PMM (monitoring_*) puis pousse vers le Pushgateway."""
    t0 = time.monotonic()
    sources_ok = []
    sources_fail = []
    results = {}

    # Scrape Scaleway en parallèle
    with ThreadPoolExecutor(max_workers=min(len(COCKPIT_PROJECTS) + 1, 10)) as pool:
        futures = {}

        # Ajoute les jobs Scaleway
        for p in COCKPIT_PROJECTS:
            futures[pool.submit(scrape_cockpit_project, p)] = ("cockpit", p["name"])

        # Ajoute le job PMM
        futures[pool.submit(scrape_pmm)] = ("pmm", "pmm")

        for future in as_completed(futures):
            source_type, name = futures[future]
            try:
                proj_name, text, duration = future.result()
                results[proj_name] = (text, duration)
            except Exception as e:
                log.error("[%s] Exception : %s", name, e)
                results[name] = (None, 0)

    # Push projet par projet
    all_ok = True
    for name, (text, duration) in results.items():
        if text:
            ok = push_to_gateway("monitoring-proxy", name, text)
            if ok:
                sources_ok.append(name)
                log.info("[%s] push OK", name)
            else:
                sources_fail.append(name)
                all_ok = False
        else:
            sources_fail.append(name)
            all_ok = False
            # Push une métrique d'erreur
            fail_metric = f"monitoring_proxy_scrape_success{{source=\"{name}\"}} 0\n"
            push_to_gateway("monitoring-proxy", name, fail_metric)

    total = time.monotonic() - t0
    log.info("Cycle terminé en %.2fs — OK: %s | FAIL: %s", total, sources_ok, sources_fail)

    with _state_lock:
        _state["last_push"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        _state["last_push_duration"] = round(total, 2)
        _state["last_push_status"] = "ok" if all_ok else "partial"
        _state["sources_ok"] = sources_ok
        _state["sources_fail"] = sources_fail


def push_loop():
    """Boucle de push en arrière-plan."""
    log.info("Démarrage de la boucle de push (intervalle : %ds)", PUSH_INTERVAL)
    while True:
        try:
            scrape_and_push()
        except Exception as e:
            log.error("Erreur inattendue dans la boucle de push : %s", e)
        time.sleep(PUSH_INTERVAL)



class ProxyHandler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        log.debug("HTTP %s", fmt % args)

    def do_GET(self):
        if self.path == "/health":
            self._handle_health()
        elif self.path == "/metrics":
            self._handle_metrics()
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not found\n")

    def _handle_health(self):
        with _state_lock:
            state = dict(_state)

        payload = json.dumps({
            "status": state["last_push_status"],
            "sources": {
                "scaleway": [p["name"] for p in COCKPIT_PROJECTS],
                "pmm": ["local"]
            },
            "metric_prefixes": {
                "cockpit": COCKPIT_METRIC_PREFIX,
                "pmm": PMM_METRIC_PREFIX,
            },
            "last_push": state["last_push"],
            "last_push_duration_seconds": state["last_push_duration"],
            "sources_ok": state["sources_ok"],
            "sources_fail": state["sources_fail"],
        }, indent=2).encode("utf-8")

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _handle_metrics(self):
        """Métriques internes du proxy."""
        with _state_lock:
            state = dict(_state)

        lines = [
            "# HELP monitoring_proxy_last_push_duration_seconds Duration of last push cycle\n",
            "# TYPE monitoring_proxy_last_push_duration_seconds gauge\n",
        ]
        if state["last_push_duration"] is not None:
            lines.append(f"monitoring_proxy_last_push_duration_seconds {state['last_push_duration']}\n")

        for name in state.get("sources_ok", []):
            lines.append(f"monitoring_proxy_source_up{{source=\"{name}\"}} 1\n")
        for name in state.get("sources_fail", []):
            lines.append(f"monitoring_proxy_source_up{{source=\"{name}\"}} 0\n")

        body = "".join(lines).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)



# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    log.info("=== Monitoring Proxy ===")
    log.info("Scaleway Cockpit projects : %d (préfixe: %s)", len(COCKPIT_PROJECTS), COCKPIT_METRIC_PREFIX)
    log.info("PMM Prometheus : %s (préfixe: %s)", PMM_PROMETHEUS_URL, PMM_METRIC_PREFIX)
    log.info("Pushgateway : %s", PUSHGATEWAY_URL)
    log.info("Intervalle de push : %ds", PUSH_INTERVAL)

    # Lancement de la boucle de push en arrière-plan
    t = threading.Thread(target=push_loop, daemon=True)
    t.start()

    # Serveur HTTP
    server = HTTPServer(("0.0.0.0", PROXY_PORT), ProxyHandler)
    log.info("Proxy démarré sur :%d", PROXY_PORT)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Arrêt.")