#!/usr/bin/env python3
"""
cockpit_proxy.py
================
Proxy HTTP qui agrège les métriques Prometheus de tous les Cockpit Scaleway
(un par projet) et les expose sur un seul endpoint /metrics.

Grafana n'a besoin que d'une seule data source Prometheus pointant sur ce proxy.

Configuration via variables d'environnement (Secret Kubernetes) :
  COCKPIT_PROJECTS   JSON : liste de projets avec leur token et leur URL
                     ex : '[{"name":"prod","url":"https://metrics.cockpit.fr-par.scaleway.com","token":"xxx"},...]'
  PROXY_PORT         Port d'écoute (défaut : 8000)
  SCRAPE_TIMEOUT     Timeout par scrape en secondes (défaut : 10)
  LOG_LEVEL          DEBUG | INFO | WARNING (défaut : INFO)

Format du label injecté : scaleway_project="<name>"

Exemple de Secret Kubernetes :
  apiVersion: v1
  kind: Secret
  metadata:
    name: cockpit-proxy-secret
  stringData:
    COCKPIT_PROJECTS: |
      [
        {"name": "default",  "url": "https://metrics.cockpit.fr-par.scaleway.com", "token": "xxx"},
        {"name": "staging",  "url": "https://metrics.cockpit.fr-par.scaleway.com", "token": "yyy"},
        {"name": "prod",     "url": "https://metrics.cockpit.nl-ams.scaleway.com", "token": "zzz"}
      ]

Endpoints exposés :
  GET /metrics   → métriques agrégées (format Prometheus text)
  GET /health    → {"status": "ok", "projects": [...]}
"""

import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, HTTPServer

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

PROXY_PORT    = int(os.environ.get("PROXY_PORT", "8000"))
SCRAPE_TIMEOUT = int(os.environ.get("SCRAPE_TIMEOUT", "10"))

def load_projects() -> list[dict]:
    """Charge la liste des projets depuis COCKPIT_PROJECTS (JSON)."""
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

# ---------------------------------------------------------------------------
# Scrape d'un projet Cockpit
# ---------------------------------------------------------------------------

# Regex pour injecter un label dans une ligne de métrique
# Gère : metric{labels} value  ET  metric value  (sans accolades)
_RE_WITH_LABELS    = re.compile(r'^(\w+)\{([^}]*)\}(.*)$')
_RE_WITHOUT_LABELS = re.compile(r'^(\w+)(\s+.*)$')


def _inject_label(line: str, label_kv: str) -> str:
    """
    Injecte label_kv (ex: 'scaleway_project="prod"') dans une ligne de métriques.
    Ignore les lignes # HELP / # TYPE / vides.
    """
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


def scrape_project(project: dict) -> tuple[str, str | None, float]:
    """
    Scrape le endpoint /metrics d'un projet Cockpit.
    Retourne (project_name, metrics_text_or_None, duration_seconds).
    """
    name  = project["name"]
    url   = project["url"].rstrip("/") + "/metrics"
    token = project["token"]
    label = f'scaleway_project="{name}"'

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

    # Injection du label scaleway_project sur chaque ligne de métrique
    lines = raw.splitlines(keepends=True)
    labeled = [_inject_label(line, label) for line in lines]

    # Méta-métrique : succès du scrape
    meta = (
        f"# HELP cockpit_proxy_scrape_success 1 if last scrape succeeded\n"
        f"# TYPE cockpit_proxy_scrape_success gauge\n"
        f"cockpit_proxy_scrape_success{{{label}}} 1\n"
        f"# HELP cockpit_proxy_scrape_duration_seconds Duration of last scrape\n"
        f"# TYPE cockpit_proxy_scrape_duration_seconds gauge\n"
        f"cockpit_proxy_scrape_duration_seconds{{{label}}} {duration:.3f}\n"
    )

    log.debug("[%s] scrape OK en %.2fs (%d lignes)", name, duration, len(lines))
    return name, "".join(labeled) + meta, duration


def scrape_all() -> str:
    """Scrape tous les projets en parallèle et concatène les résultats."""
    if not PROJECTS:
        return "# ERROR: no projects configured\n"

    parts = []
    with ThreadPoolExecutor(max_workers=min(len(PROJECTS), 10)) as pool:
        futures = {pool.submit(scrape_project, p): p["name"] for p in PROJECTS}
        for future in as_completed(futures):
            name, text, duration = future.result()
            if text:
                parts.append(text)
            else:
                label = f'scaleway_project="{name}"'
                parts.append(
                    f"cockpit_proxy_scrape_success{{{label}}} 0\n"
                    f"cockpit_proxy_scrape_duration_seconds{{{label}}} {duration:.3f}\n"
                )

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Serveur HTTP
# ---------------------------------------------------------------------------

class ProxyHandler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):  # silence le logger HTTP par défaut
        log.debug("HTTP %s", fmt % args)

    def do_GET(self):
        if self.path == "/metrics":
            self._handle_metrics()
        elif self.path == "/health":
            self._handle_health()
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not found\n")

    def _handle_metrics(self):
        t0 = time.monotonic()
        body = scrape_all().encode("utf-8")
        duration = time.monotonic() - t0
        log.info("/metrics agrégé en %.2fs (%d bytes)", duration, len(body))

        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle_health(self):
        payload = json.dumps({
            "status": "ok",
            "projects": [p["name"] for p in PROJECTS],
            "project_count": len(PROJECTS),
        }).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if not PROJECTS:
        log.error("Aucun projet configuré — vérifier COCKPIT_PROJECTS.")
        raise SystemExit(1)

    server = HTTPServer(("0.0.0.0", PROXY_PORT), ProxyHandler)
    log.info("Proxy démarré sur :%d", PROXY_PORT)
    log.info("Projets : %s", [p["name"] for p in PROJECTS])
    log.info("Endpoints : /metrics  /health")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Arrêt.")