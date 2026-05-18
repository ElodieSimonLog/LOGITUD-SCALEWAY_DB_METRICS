#!/usr/bin/env python3
"""
cockpit_proxy.py
================
Agrège les métriques Prometheus de tous les Cockpit Scaleway et les pousse
vers un Pushgateway Prometheus toutes les PUSH_INTERVAL secondes.

Le /federate Scaleway renvoie plusieurs points dans le temps pour une même
série — ce script déduplique et ne garde que la dernière valeur par série
avant de pusher, ce qui est requis par le Pushgateway.

Expose aussi :
  GET /health    → {"status": "ok", "projects": [...], "last_push": "..."}
  GET /metrics   → métriques internes du proxy (durée, succès par projet)

Configuration via variables d'environnement :
  COCKPIT_PROJECTS   JSON : liste de projets
                     ex : '[{"name":"prod","url":"https://xxx.metrics.cockpit.fr-par.scw.cloud","token":"xxx"},...]'
  PUSHGATEWAY_URL    URL du Pushgateway
                     (défaut : http://prometheus-pushgateway.grafana.svc.cluster.local:9091)
  PUSH_INTERVAL      Intervalle de push en secondes (défaut : 60)
  PROXY_PORT         Port d'écoute pour /health et /metrics internes (défaut : 8000)
  SCRAPE_TIMEOUT     Timeout par scrape en secondes (défaut : 30)
  LOG_LEVEL          DEBUG | INFO | WARNING (défaut : INFO)
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


def load_projects() -> list[dict]:
    raw = os.environ.get("COCKPIT_PROJECTS", "")
    if not raw:
        log.error("Variable COCKPIT_PROJECTS manquante ou vide.")
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
    log.info("Projets chargés : %s", [p["name"] for p in projects])
    return projects


PROJECTS = load_projects()

# État partagé (thread-safe via lock)
_state_lock = threading.Lock()
_state = {
    "last_push": None,
    "last_push_duration": None,
    "last_push_status": "pending",
    "projects_ok": [],
    "projects_fail": [],
}

# ---------------------------------------------------------------------------
# Scrape d'un projet Cockpit
# ---------------------------------------------------------------------------

_RE_WITH_LABELS    = re.compile(r'^(\w+)\{([^}]*)\}(.*)$')
_RE_WITHOUT_LABELS = re.compile(r'^(\w+)(\s+.*)$')


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


def _deduplicate(lines: list[str]) -> list[str]:
    """
    Déduplique les lignes de métriques.

    Le /federate Scaleway renvoie plusieurs points dans le temps pour une
    même série (même nom + mêmes labels, valeurs différentes). Le Pushgateway
    n'accepte qu'une seule valeur par série — on garde la dernière occurrence.

    Les lignes # HELP et # TYPE sont dédupliquées par nom de métrique.
    """
    help_lines = {}   # metric_name -> ligne # HELP
    type_lines = {}   # metric_name -> ligne # TYPE
    metric_lines = {} # "metric_name{labels}" -> dernière ligne de valeur

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
            # Clé = tout ce qui précède la valeur (metric_name{labels})
            # Format : metric_name{labels} value [timestamp]
            # On sépare sur le premier espace après les accolades fermantes
            if "{" in stripped:
                brace_end = stripped.index("}") + 1
                key = stripped[:brace_end]
            else:
                key = stripped.split(" ")[0]
            metric_lines[key] = line  # écrase les anciennes valeurs

    # Reconstruit dans l'ordre : HELP, TYPE, valeurs
    result = []
    result.extend(help_lines.values())
    result.extend(type_lines.values())
    result.extend(metric_lines.values())
    return result


def scrape_project(project: dict) -> tuple[str, str | None, float]:
    name  = project["name"]
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
        log.warning("[%s] HTTP %s lors du scrape : %s", name, e.code, url)
        return name, None, time.monotonic() - t0
    except Exception as e:
        log.warning("[%s] Erreur scrape : %s", name, e)
        return name, None, time.monotonic() - t0

    duration = time.monotonic() - t0

    # Injection du label scaleway_project sur chaque ligne
    lines = raw.splitlines(keepends=True)
    labeled = [_inject_label(line, label) for line in lines]

    # Déduplication — le /federate renvoie plusieurs points par série
    deduped = _deduplicate(labeled)
    log.debug(
        "[%s] scrape OK en %.2fs — %d lignes brutes → %d après dédup",
        name, duration, len(lines), len(deduped)
    )

    meta = (
        f"# HELP cockpit_proxy_scrape_success 1 if last scrape succeeded\n"
        f"# TYPE cockpit_proxy_scrape_success gauge\n"
        f"cockpit_proxy_scrape_success{{{label}}} 1\n"
        f"# HELP cockpit_proxy_scrape_duration_seconds Duration of last scrape\n"
        f"# TYPE cockpit_proxy_scrape_duration_seconds gauge\n"
        f"cockpit_proxy_scrape_duration_seconds{{{label}}} {duration:.3f}\n"
    )

    return name, "".join(deduped) + meta, duration


# ---------------------------------------------------------------------------
# Push vers le Pushgateway
# ---------------------------------------------------------------------------

def push_to_gateway(job: str, instance: str, data: str) -> bool:
    """Pousse les métriques vers le Pushgateway via PUT /metrics/job/<job>/instance/<instance>."""
    url = f"{PUSHGATEWAY_URL}/metrics/job/{job}/instance/{instance}"
    body = data.encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="PUT",
        headers={"Content-Type": "text/plain; version=0.0.4; charset=utf-8"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            log.debug("Push OK vers %s : HTTP %s", url, resp.status)
            return True
    except urllib.error.HTTPError as e:
        log.error("Erreur push [%s] HTTP %s : %s", instance, e.code, e.read().decode()[:200])
        return False
    except Exception as e:
        log.error("Erreur push [%s] : %s", instance, e)
        return False


# ---------------------------------------------------------------------------
# Boucle principale
# ---------------------------------------------------------------------------

def scrape_and_push():
    """Scrape tous les projets en parallèle puis pousse par projet vers le Pushgateway."""
    if not PROJECTS:
        log.error("Aucun projet configuré.")
        return

    t0 = time.monotonic()
    projects_ok = []
    projects_fail = []

    # Scrape en parallèle — tous les projets au même moment
    results = {}
    with ThreadPoolExecutor(max_workers=min(len(PROJECTS), 10)) as pool:
        futures = {pool.submit(scrape_project, p): p for p in PROJECTS}
        for future in as_completed(futures):
            project = futures[future]
            name, text, duration = future.result()
            results[name] = (text, duration)

    # Push projet par projet — évite un payload géant
    all_ok = True
    for name, (text, duration) in results.items():
        if text:
            ok = push_to_gateway("cockpit-proxy", name, text)
            if ok:
                projects_ok.append(name)
                log.info("[%s] push OK", name)
            else:
                projects_fail.append(name)
                all_ok = False
        else:
            projects_fail.append(name)
            all_ok = False
            label = f'scaleway_project="{name}"'
            fail_metric = (
                f"cockpit_proxy_scrape_success{{{label}}} 0\n"
                f"cockpit_proxy_scrape_duration_seconds{{{label}}} {duration:.3f}\n"
            )
            push_to_gateway("cockpit-proxy", name, fail_metric)

    total = time.monotonic() - t0
    log.info("Cycle terminé en %.2fs — OK: %s | FAIL: %s", total, projects_ok, projects_fail)

    with _state_lock:
        _state["last_push"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        _state["last_push_duration"] = round(total, 2)
        _state["last_push_status"] = "ok" if all_ok else "partial"
        _state["projects_ok"] = projects_ok
        _state["projects_fail"] = projects_fail


def push_loop():
    """Boucle de push en arrière-plan."""
    log.info("Démarrage de la boucle de push (intervalle : %ds)", PUSH_INTERVAL)
    while True:
        try:
            scrape_and_push()
        except Exception as e:
            log.error("Erreur inattendue dans la boucle de push : %s", e)
        time.sleep(PUSH_INTERVAL)


# ---------------------------------------------------------------------------
# Serveur HTTP (health + métriques internes)
# ---------------------------------------------------------------------------

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
            "projects": [p["name"] for p in PROJECTS],
            "project_count": len(PROJECTS),
            "last_push": state["last_push"],
            "last_push_duration_seconds": state["last_push_duration"],
            "projects_ok": state["projects_ok"],
            "projects_fail": state["projects_fail"],
        }, indent=2).encode("utf-8")

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _handle_metrics(self):
        """Métriques internes du proxy (pas les métriques Cockpit)."""
        with _state_lock:
            state = dict(_state)

        lines = [
            "# HELP cockpit_proxy_last_push_duration_seconds Duration of last push cycle\n",
            "# TYPE cockpit_proxy_last_push_duration_seconds gauge\n",
        ]
        if state["last_push_duration"] is not None:
            lines.append(f"cockpit_proxy_last_push_duration_seconds {state['last_push_duration']}\n")

        for name in state.get("projects_ok", []):
            label = f'scaleway_project="{name}"'
            lines.append(f"cockpit_proxy_project_up{{{label}}} 1\n")
        for name in state.get("projects_fail", []):
            label = f'scaleway_project="{name}"'
            lines.append(f"cockpit_proxy_project_up{{{label}}} 0\n")

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
    if not PROJECTS:
        log.error("Aucun projet configuré — vérifier COCKPIT_PROJECTS.")
        raise SystemExit(1)

    # Lancement de la boucle de push en arrière-plan
    t = threading.Thread(target=push_loop, daemon=True)
    t.start()

    # Serveur HTTP pour /health et /metrics internes
    server = HTTPServer(("0.0.0.0", PROXY_PORT), ProxyHandler)
    log.info("Proxy démarré sur :%d", PROXY_PORT)
    log.info("Projets : %s", [p["name"] for p in PROJECTS])
    log.info("Pushgateway : %s", PUSHGATEWAY_URL)
    log.info("Intervalle de push : %ds", PUSH_INTERVAL)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Arrêt.")