# plugins/aks_status_plugin.py
from __future__ import annotations

from typing import Dict, List, Optional, Tuple
from io import StringIO
import csv
import re

from flask import Blueprint, request, render_template_string, Response
from flask_appbuilder import BaseView as AppBuilderBaseView, expose
from airflow.plugins_manager import AirflowPlugin
from airflow.www.decorators import action_logging

# Config
DEFAULT_LIMIT = 300
MAX_LIMIT = 2000

HEADERS = [
    "Namespace",
    "Node Pool",
    "Node",
    "Machine Type",
    "Component (Pod)",
    "Container",
    "Status",
    "CPU Request",
    "Memory Request",
    "CPU Limit",
    "Memory Limit",
]

HEADER_KEY = {h: h for h in HEADERS}

TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>AKS Cluster Info</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    html,body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;font-size:14px;margin:0;padding:0;color:#111}
    .wrap{padding:16px}
    .toolbar{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin:12px 0}
    input,button{font-size:14px;padding:6px 8px}
    input[type="number"]{width:100px}
    .alert{background:#fdecea;color:#611a15;border:1px solid #f5c6cb;padding:8px 10px;border-radius:4px;margin:12px 0}
    .tablewrap{max-height:70vh;overflow:auto;border:1px solid #ddd;border-radius:4px}
    table{border-collapse:collapse;width:100%}
    th,td{border:1px solid #ddd;padding:6px 8px;text-align:left;vertical-align:top;white-space:nowrap}
    thead th{position:sticky;top:0;background:#f7f7f7}
    tbody tr:nth-child(even){background:#fafafa}
    .muted{color:#666}
    a.link{color:inherit;text-decoration:none}
    .sorthint{font-size:11px;margin-left:4px;color:#666}
  </style>
</head>
<body>
<div class="wrap">
  <h2>AKS Cluster Info</h2>

  {% if warning %}
    <div class="alert">{{ warning }}</div>
  {% endif %}

  <form method="get" class="toolbar" id="filters">
    <label>Namespace
      <input type="text" name="namespace" value="{{ ns_query }}" placeholder="(all)">
    </label>
    <label>Container
      <input type="text" name="container" value="{{ container_query }}" placeholder="(all)">
    </label>
    <label>Limit
      <input type="number" name="limit" min="1" max="{{ max_limit }}" value="{{ limit }}">
    </label>
    <input type="hidden" name="sort_by" value="{{ sort_by }}">
    <input type="hidden" name="order" value="{{ order }}">
    <button type="submit">Apply</button>
    <a href="?" style="margin-left:6px;">Clear</a>
    <a style="margin-left:6px;"
       href="?namespace={{ ns_query|urlencode }}&container={{ container_query|urlencode }}&limit={{ limit }}&sort_by={{ sort_by|urlencode }}&order={{ order|urlencode }}&format=csv">
       Download CSV
    </a>
  </form>

  <div class="tablewrap">
    <table>
      <thead>
        <tr>
          {% for h in headers %}
            {% set is_sorted = (sort_by == h) %}
            {% set next_order = 'desc' if is_sorted and order == 'asc' else 'asc' %}
            <th>
              <a class="link"
                 href="?namespace={{ ns_query|urlencode }}&container={{ container_query|urlencode }}&limit={{ limit }}&sort_by={{ h|urlencode }}&order={{ next_order }}">
                {{ h }}{% if is_sorted %}<span class="sorthint">[{{ order }}]</span>{% endif %}
              </a>
            </th>
          {% endfor %}
        </tr>
      </thead>
      <tbody>
        {% if rows %}
          {% for r in rows %}
            <tr>
              <td>{{ r["Namespace"] }}</td>
              <td>{{ r["Node Pool"] }}</td>
              <td>{{ r["Node"] }}</td>
              <td>{{ r["Machine Type"] }}</td>
              <td>{{ r["Component (Pod)"] }}</td>
              <td>{{ r["Container"] }}</td>
              <td>{{ r["Status"] }}</td>
              <td>{{ r["CPU Request"] }}</td>
              <td>{{ r["Memory Request"] }}</td>
              <td>{{ r["CPU Limit"] }}</td>
              <td>{{ r["Memory Limit"] }}</td>
            </tr>
          {% endfor %}
        {% else %}
          <tr><td class="muted" colspan="{{ headers|length }}">No data found.</td></tr>
        {% endif %}
      </tbody>
    </table>
  </div>
</div>
</body>
</html>
"""

bp = Blueprint("aks_cluster_info_inline", __name__)

def _load_k8s():
    try:
        from kubernetes import client, config
        return client, config
    except Exception:
        return None, None

def _try_load_kube_config(config_mod):
    try:
        config_mod.load_incluster_config()
        return "in-cluster"
    except Exception:
        pass
    try:
        config_mod.load_kube_config()
        return "kubeconfig"
    except Exception as e:
        raise RuntimeError("Kubernetes config load failed: {e}".format(e=e))

def _format_qty(qty: Optional[str]) -> str:
    return "" if not qty else str(qty)

_CPU_M = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*m\s*$", re.IGNORECASE)
_CPU_NUM = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*$")

def _parse_cpu(cpu: str | None) -> float:
    if not cpu:
        return float("-inf")
    s = cpu.strip()
    m = _CPU_M.match(s)
    if m:
        return float(m.group(1)) / 1000.0
    m = _CPU_NUM.match(s)
    if m:
        return float(m.group(1))
    return float("-1e308")

_MEM_UNITS_2 = {"KI":1024**1,"MI":1024**2,"GI":1024**3,"TI":1024**4,"PI":1024**5,"EI":1024**6}
_MEM_UNITS_10 = {"K":10**3,"M":10**6,"G":10**9,"T":10**12,"P":10**15,"E":10**18}

def _parse_mem(mem: str | None) -> float:
    if not mem:
        return float("-inf")
    s = mem.strip()
    m = re.match(r"^\s*(\d+(?:\.\d+)?)\s*([kKmMgGtTpPeE]i)\s*$", s)
    if m:
        return float(m.group(1)) * _MEM_UNITS_2[m.group(2).upper()]
    m = re.match(r"^\s*(\d+(?:\.\d+)?)\s*([kKmMgGtTpPeE])\s*$", s)
    if m:
        return float(m.group(1)) * _MEM_UNITS_10[m.group(2).upper()]
    m = re.match(r"^\s*(\d+(?:\.\d+)?)\s*$", s)
    if m:
        return float(m.group(1))
    return float("-1e308")

def _container_resources(cspec) -> Tuple[str, str, str, str]:
    res = getattr(cspec, "resources", None)
    req = (getattr(res, "requests", None) or {}) if res else {}
    lim = (getattr(res, "limits", None) or {}) if res else {}
    return (
        _format_qty(req.get("cpu")),
        _format_qty(req.get("memory")),
        _format_qty(lim.get("cpu")),
        _format_qty(lim.get("memory")),
    )

def _container_status_str(cstatus) -> str:
    if not cstatus or not cstatus.state:
        return ""
    st = cstatus.state
    if getattr(st, "waiting", None):
        return "Waiting({r})".format(r=(st.waiting.reason or "Waiting"))
    if getattr(st, "terminated", None):
        return "Terminated({r}, code={c})".format(r=(st.terminated.reason or "Terminated"), c=st.terminated.exit_code)
    if getattr(st, "running", None):
        return "Running"
    return f"{cstatus.state}"

_VMSS_FROM_PROVIDER = re.compile(r"/virtualMachineScaleSets/([^/]+)/", re.IGNORECASE)

def _extract_vmss_from_provider_id(provider_id: str | None) -> Optional[str]:
    if not provider_id:
        return None
    m = _VMSS_FROM_PROVIDER.search(provider_id)
    return m.group(1) if m else None

def _derive_pool_from_vmss(vmss: str | None) -> Optional[str]:
    if not vmss:
        return None
    s = vmss
    if s.lower().startswith("aks-"):
        s = s[4:]
    elif s.lower().startswith("aks"):
        s = s[3:]
    s = re.sub(r"-?vmss$", "", s, flags=re.IGNORECASE)
    seg = s.split("-")[0]
    if seg and seg.lower() != "agentpool":
        return seg
    return None

def _derive_pool_from_node_name(node_name: str | None) -> Optional[str]:
    if not node_name:
        return None
    m = re.match(r"^aks-([a-z0-9]+)-", node_name)
    if m and m.group(1).lower() != "agentpool":
        return m.group(1)
    return None

def _best_effort_node_pool(labels: Dict[str, str], node_name: str, provider_id: str | None) -> str:
    for key in ("kubernetes.azure.com/agentpool", "agentpool"):
        val = labels.get(key)
        if val:
            if val.lower() != "agentpool":
                return val
            vmss = _extract_vmss_from_provider_id(provider_id)
            derived = _derive_pool_from_vmss(vmss) or _derive_pool_from_node_name(node_name)
            return derived or val
    for key in ("cloud.google.com/gke-nodepool", "eks.amazonaws.com/nodegroup", "nodepool"):
        val = labels.get(key)
        if val:
            return val
    vmss = _extract_vmss_from_provider_id(provider_id)
    derived = _derive_pool_from_vmss(vmss) or _derive_pool_from_node_name(node_name)
    return derived or ""

def _load_k8s():
    try:
        from kubernetes import client, config
        return client, config
    except Exception:
        return None, None

def _try_load_kube_config(config_mod):
    try:
        config_mod.load_incluster_config()
        return "in-cluster"
    except Exception:
        pass
    try:
        config_mod.load_kube_config()
        return "kubeconfig"
    except Exception as e:
        raise RuntimeError("Kubernetes config load failed: {e}".format(e=e))

def _fetch_rows(limit: int, ns_filter: Optional[str], container_filter: Optional[str]):
    client, config = _load_k8s()
    if not client or not config:
        return [], "Python package 'kubernetes' is not installed in the webserver environment."

    try:
        _try_load_kube_config(config)
    except Exception as e:
        return [], str(e)

    v1 = client.CoreV1Api()

    node_map: Dict[str, Tuple[str, str]] = {}
    try:
        for n in v1.list_node().items:
            labels = n.metadata.labels or {}
            provider_id = getattr(getattr(n, "spec", None), "provider_id", None)
            node_pool = _best_effort_node_pool(labels, n.metadata.name, provider_id)
            machine_type = (
                labels.get("node.kubernetes.io/instance-type")
                or labels.get("beta.kubernetes.io/instance-type")
                or ""
            )
            node_map[n.metadata.name] = (node_pool, machine_type)
    except Exception:
        node_map = {}

    rows: List[Dict[str, str]] = []
    try:
        plist = v1.list_pod_for_all_namespaces(limit=limit)
        for p in plist.items:
            if ns_filter and p.metadata.namespace != ns_filter:
                continue
            pod_name = p.metadata.name
            namespace = p.metadata.namespace
            node_name = p.spec.node_name or ""
            node_pool, machine_type = node_map.get(node_name, ("", ""))

            spec_by_name = {c.name: c for c in (p.spec.containers or [])}
            status_by_name = {cs.name: cs for cs in (p.status.container_statuses or []) or []}

            for cname, cspec in spec_by_name.items():
                if container_filter and container_filter not in cname:
                    continue
                cstatus = status_by_name.get(cname)
                status_str = _container_status_str(cstatus)
                cpu_req, mem_req, cpu_lim, mem_lim = _container_resources(cspec)
                rows.append(
                    {
                        "Namespace": namespace,
                        "Node Pool": node_pool,
                        "Node": node_name,
                        "Machine Type": machine_type,
                        "Component (Pod)": pod_name,
                        "Container": cname,
                        "Status": status_str,
                        "CPU Request": cpu_req,
                        "Memory Request": mem_req,
                        "CPU Limit": cpu_lim,
                        "Memory Limit": mem_lim,
                    }
                )
        return rows, None

    except Exception as e:
        return [], "Unexpected error: {e}".format(e=e)

def _row_sort_key(row: Dict[str, str], sort_by: str) -> tuple:
    col = HEADER_KEY.get(sort_by, "Namespace")
    val = row.get(col, "")
    if col in ("CPU Request", "CPU Limit"):
        return (_parse_cpu(val), row["Namespace"], row["Component (Pod)"], row["Container"])
    if col in ("Memory Request", "Memory Limit"):
        return (_parse_mem(val), row["Namespace"], row["Component (Pod)"], row["Container"])
    return (val.lower(), row["Namespace"], row["Component (Pod)"], row["Container"])

class AKSClusterInfoInlineView(AppBuilderBaseView):
    route_base = "/aks-cluster-info"
    default_view = "index"

    @expose("/")
    @action_logging
    def index(self):
        ns_query = (request.args.get("namespace") or "").strip()
        container_query = (request.args.get("container") or "").strip()
        limit = request.args.get("limit", type=int) or DEFAULT_LIMIT
        if limit < 1:
            limit = 1
        if limit > MAX_LIMIT:
            limit = MAX_LIMIT

        sort_by = request.args.get("sort_by") or "Namespace"
        order = (request.args.get("order") or "asc").lower()
        if sort_by not in HEADER_KEY:
            sort_by = "Namespace"
        if order not in ("asc", "desc"):
            order = "asc"

        if (request.args.get("format") or "").lower() == "csv":
            rows, warn = _fetch_rows(
                limit=limit,
                ns_filter=ns_query or None,
                container_filter=container_query or None,
            )
            rows.sort(key=lambda r: _row_sort_key(r, sort_by), reverse=(order == "desc"))
            sio = StringIO()
            writer = csv.writer(sio)
            writer.writerow(HEADERS)
            for r in rows:
                writer.writerow([
                    r["Namespace"],
                    r["Node Pool"],
                    r["Node"],
                    r["Machine Type"],
                    r["Component (Pod)"],
                    r["Container"],
                    r["Status"],
                    r["CPU Request"],
                    r["Memory Request"],
                    r["CPU Limit"],
                    r["Memory Limit"],
                ])
            return Response(
                sio.getvalue(),
                mimetype="text/csv",
                headers={"Content-Disposition": "attachment; filename=aks_cluster_info.csv"},
            )

        rows, warn = _fetch_rows(
            limit=limit,
            ns_filter=ns_query or None,
            container_filter=container_query or None,
        )
        rows.sort(key=lambda r: _row_sort_key(r, sort_by), reverse=(order == "desc"))

        return render_template_string(
            TEMPLATE,
            headers=HEADERS,
            rows=rows,
            warning=warn,
            ns_query=ns_query,
            container_query=container_query,
            limit=limit,
            max_limit=MAX_LIMIT,
            sort_by=sort_by,
            order=order,
        )

class AKSClusterInfoPlugin(AirflowPlugin):
    name = "aks_cluster_info_plugin_inline"
    appbuilder_views = [
        {
            "name": "AKS Cluster Info",
            "category": "Custom",
            "view": AKSClusterInfoInlineView(),
        }
    ]
    flask_blueprints = [bp]