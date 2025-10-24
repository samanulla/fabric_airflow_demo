#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AksPodLogs - Airflow 2.10.5 plugin (test environment)
Custom -> Pod Logs
Two fields only: Namespace and Pod name. Fetches logs for all containers in the pod.

Requirements:
  pip install "kubernetes>=26,<27"

Kubernetes access for the Airflow webserver:
  - In-cluster: ServiceAccount needs:
      verbs: ["get","list"] on resources ["pods"]
      verbs: ["get"]        on resources ["pods/log"]
  - Out-of-cluster: set KUBECONFIG and ensure network access.
"""

from __future__ import annotations

import os
import traceback
from typing import Dict, List, Optional

from airflow.plugins_manager import AirflowPlugin
from flask import Response, request
from flask_appbuilder import BaseView, expose


# ---------------------- Kubernetes client helper ----------------------

def _get_k8s_core_v1():
    """
    Return kubernetes.client.CoreV1Api with in-cluster config first,
    then fall back to kubeconfig if available.
    """
    try:
        from kubernetes import client, config
        try:
            config.load_incluster_config()
        except Exception:
            kubeconfig = os.environ.get("KUBECONFIG")
            config.load_kube_config(config_file=kubeconfig)
        return client.CoreV1Api()
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "Kubernetes client not available or configuration failed. "
            "Install 'kubernetes>=26,<27' and ensure RBAC/KUBECONFIG is set."
        ) from e


# ---------------------------- Inline HTML -----------------------------

DEFAULT_NS = os.environ.get("AIRFLOW__KUBERNETES__NAMESPACE", "default")

# Plain triple-quoted string (NOT an f-string). We replace __DEFAULT_NS__ and __BASE_PATH__ at runtime.
PAGE_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Pod Logs</title>
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin:16px; }
    fieldset { border:1px solid #ddd; padding:12px; margin-bottom:12px; border-radius:8px; }
    label { display:inline-block; min-width:120px; }
    input { padding:4px 6px; }
    button { padding:6px 10px; cursor:pointer; }
    #logs { white-space:pre; background:#111; color:#ddd; padding:12px; border-radius:6px; overflow:auto; max-height:70vh; }
    .row { margin:6px 0; display:flex; gap:10px; align-items:center; flex-wrap:wrap; }
    .muted { color:#666; font-size:12px; }
  </style>
</head>
<body>
  <h2>Pod Logs</h2>
  <form id="controls" onsubmit="return false;">
    <fieldset>
      <div class="row">
        <label for="ns">Namespace</label>
        <input id="ns" value="__DEFAULT_NS__" />
        <label for="pod">Pod</label>
        <input id="pod" placeholder="example-pod-abc123" style="min-width:320px;" />
        <button id="fetch">Fetch Logs</button>
        <span id="status" class="muted"></span>
      </div>
    </fieldset>
  </form>

  <div id="logs">(no logs yet)</div>

<script>
const el = (id) => document.getElementById(id);

el('fetch').addEventListener('click', async () => {
  const ns = el('ns').value || 'default';
  const pod = el('pod').value.trim();
  const statusEl = el('status');
  const logsEl = el('logs');

  if (!pod) { statusEl.textContent = "Pod name is required."; return; }
  statusEl.textContent = "Fetching logs...";

  try {
    const u = new URL(window.location.origin + "__BASE_PATH__/get");
    u.searchParams.set('namespace', ns);
    u.searchParams.set('pod', pod);

    const r = await fetch(u.toString(), { credentials: 'same-origin' });
    const txt = await r.text();
    logsEl.textContent = txt || "(empty)";
    statusEl.textContent = r.ok ? "Done." : "Error " + r.status;
  } catch (e) {
    logsEl.textContent = "Error: " + e.message;
    statusEl.textContent = "Error while fetching logs.";
  }
});
</script>
</body>
</html>
"""


# ------------------------------- View --------------------------------

class AksPodLogsView(BaseView):
    """
    Test-only view (no Airflow access control decorators as requested).
    """
    route_base = "/aks-pod-logs"
    default_view = "index"

    @expose("/")
    def index(self):
        base_path = request.script_root + self.route_base
        html = (
            PAGE_HTML
            .replace("__BASE_PATH__", base_path.rstrip("/"))
            .replace("__DEFAULT_NS__", DEFAULT_NS)
        )
        return Response(html, mimetype="text/html")

    @expose("/get", methods=["GET"])
    def get(self):
        ns = request.args.get("namespace", "default")
        pod = request.args.get("pod")

        if not pod:
            return Response("Missing 'pod' parameter", status=400, mimetype="text/plain")

        # Server-side defaults for a concise test experience
        tail_lines: Optional[int] = 500
        previous: bool = False
        timestamps: bool = True

        try:
            core = _get_k8s_core_v1()
            # Read pod to discover containers (including init containers)
            p = core.read_namespaced_pod(name=pod, namespace=ns)

            containers: List[str] = []
            if p.spec and p.spec.containers:
                containers.extend([c.name for c in p.spec.containers])
            if p.spec and p.spec.init_containers:
                for c in p.spec.init_containers:
                    if c.name not in containers:
                        containers.append(c.name)

            if not containers:
                return Response("(no containers found)", mimetype="text/plain")

            parts: List[str] = []
            for c in containers:
                try:
                    text = core.read_namespaced_pod_log(
                        name=pod,
                        namespace=ns,
                        container=c,
                        tail_lines=tail_lines,
                        previous=previous,
                        timestamps=timestamps,
                    ) or ""
                except Exception as le:
                    # Surface container-specific issues but keep collecting others
                    text = f"[error fetching logs for container {c}: {getattr(le, 'status', '')} {getattr(le, 'reason', '')}]\\n{getattr(le, 'body', '')}"
                parts.append(f"===== container: {c} =====\\n{text}\\n")

            body = "\\n".join(parts)
            headers: Dict[str, str] = {}
            return Response(body, headers=headers, mimetype="text/plain")

        except Exception as e:
            body = getattr(e, "body", "")
            status = getattr(e, "status", 500) or 500
            msg = f"Error fetching logs for {ns}/{pod}: {e}\\n"
            if body:
                msg += f"{body}\\n"
            msg += traceback.format_exc()
            return Response(msg, status=status, mimetype="text/plain")


# --------------------------- Plugin registration ----------------------------

class AksPodLogsPlugin(AirflowPlugin):
    name = "aks_pod_logs_plugin"
    appbuilder_views = [
        {
            "name": "Pod Logs",
            "category": "Custom",
            "view": AksPodLogsView(),
        }
    ]
