"""i3 Admin Portal — receives LogEvents, watches SIP state notifications
across multiple i3 FE nodes (LVF, ECRF, MCS, GCS, MDS — anything built on
i3-fe-core), and shows it all live on one dashboard.

Run: uvicorn app.main:app --reload --port 9100
Configure targets in nodes.json (see nodes.json.example), then point each
node's LVF_LOGGING_SERVICE_URI (or FE equivalent) at this portal:
  LVF_LOGGING_SERVICE_URI=http://localhost:9100
"""

from __future__ import annotations

import json
import logging
import os
import xml.etree.ElementTree as ET
from contextlib import asynccontextmanager
from dataclasses import asdict

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from app.nodes import Node, load_nodes
from app.sip_subscriber import PersistentSipSubscriber
from app.store import store

logging.basicConfig(level=os.environ.get("PORTAL_LOG_LEVEL", "INFO"))
log = logging.getLogger("i3_admin_portal")

templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

nodes: list[Node] = []
subscribers: dict[str, PersistentSipSubscriber] = {}
_element_id_index: dict[str, Node] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    global nodes, _element_id_index
    nodes_file = os.environ.get("PORTAL_NODES_FILE", "nodes.json")
    nodes = load_nodes(nodes_file)
    _element_id_index = {n.element_id: n for n in nodes if n.element_id}

    if not nodes:
        log.warning("No nodes configured (%s) — LogEvents still accepted, no SIP watching", nodes_file)

    for node in nodes:
        if not node.has_sip:
            continue
        sub = PersistentSipSubscriber(node=node, store=store)
        await sub.start()
        subscribers[node.name] = sub
        log.info("SIP subscriber watching %s (%s) at %s:%s", node.name, node.role, node.sip_host, node.sip_port)

    yield

    for sub in subscribers.values():
        await sub.stop()


app = FastAPI(title="i3 Admin Portal", lifespan=lifespan)


_LOG_EVENT_KIND = {
    "LostQueryLogEvent": "log_query",
    "LostResponseLogEvent": "log_response",
    "ElementStateChangeLogEvent": "log_state",
    "ServiceStateChangeLogEvent": "log_state",
    "SubscribeLogEvent": "log_subscribe",
    "DiscrepancyReportLogEvent": "log_dr",
    "VersionsLogEvent": "log_other",
}

# A LogEvent for one of these types is proof the node is actively dispatching
# the corresponding SIP event package right now — a stronger, immediate
# signal than waiting for the next scheduled initial-subscribe retry tick.
_LOG_EVENT_TO_PACKAGE = {
    "ElementStateChangeLogEvent": "emergency-ElementState",
    "ServiceStateChangeLogEvent": "emergency-ServiceState",
}

# §3.7.1 DiscrepancyReport MANDATORY prolog fields (i3-fe-core
# discrepancy/models.py DiscrepancyReport._require call in from_dict).
_DR_REQUIRED_FIELDS = (
    "resolutionUri",
    "reportType",
    "discrepancyReportSubmittalTimeStamp",
    "discrepancyReportId",
    "reportingAgencyName",
    "reportingContactJcard",
    "problemSeverity",
)

# This portal's own identity for the MANDATORY DiscrepancyReportResponse
# fields (§3.7.1) — it isn't a real i3 element, so a minimal, clearly-labeled
# jCard (RFC 7095) is enough for a conformant reporter to accept the 201.
_DR_RESPONDING_AGENCY_NAME = "i3-admin-portal"
_DR_RESPONDING_CONTACT_JCARD = [
    "vcard",
    [
        ["version", {}, "text", "4.0"],
        ["fn", {}, "text", _DR_RESPONDING_AGENCY_NAME],
        ["kind", {}, "text", "org"],
    ],
]


def _xml_root_local_name(xml_str: str | None) -> str | None:
    """Local (namespace-stripped) name of an XML string's root element, e.g.
    "findService" out of "<ns0:findService xmlns:ns0="...">...". Used to
    label LoST queryAdapter/responseAdapter blobs with their actual
    operation. Defensive: any empty/None/unparseable input returns None
    rather than raising — this only ever feeds a display label."""
    if not xml_str:
        return None
    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError:
        return None
    tag = root.tag
    return tag.split("}", 1)[1] if tag.startswith("{") else tag


def _classify(body: dict) -> tuple[str, str, str, str, str | None]:
    """Returns (kind, summary, source, role, operation). Source/role come
    from matching the LogEvent's elementId against nodes.json; unregistered
    senders still show up, just tagged with their raw elementId/agencyId and
    role '?'. operation is the LoST root element's local name (findService,
    findServiceResponse, etc.) for LostQuery/LostResponseLogEvent, else None."""
    event_type = body.get("logEventType", "Unknown")
    direction = body.get("direction", "")
    kind = _LOG_EVENT_KIND.get(event_type, "log_other")

    element_id = body.get("elementId")
    node = _element_id_index.get(element_id) if element_id else None
    if node:
        source, role = node.name, node.role
    else:
        source, role = element_id or body.get("agencyId") or "unknown", "?"

    operation = None
    if event_type == "LostQueryLogEvent":
        operation = _xml_root_local_name(body.get("queryAdapter"))
        summary = f"{direction} query {body.get('queryId', '')}"
    elif event_type == "LostResponseLogEvent":
        operation = _xml_root_local_name(body.get("responseAdapter"))
        summary = f"{direction} response {body.get('responseId', '')}"
    elif event_type == "ElementStateChangeLogEvent":
        summary = f"{direction} ElementState change"
    elif event_type == "ServiceStateChangeLogEvent":
        summary = f"{direction} ServiceState → {body.get('newState', '?')}"
    elif event_type == "SubscribeLogEvent":
        summary = f"{direction} SUBSCRIBE {body.get('package', '')} ({body.get('purpose', '')})"
    elif event_type == "DiscrepancyReportLogEvent":
        summary = f"{direction} DR {body.get('type', '')}"
    elif event_type == "VersionsLogEvent":
        summary = f"{direction} Versions"
    else:
        summary = event_type
    return kind, summary, source, role, operation


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {"events": [asdict(e) for e in store.recent(300)]},
    )


@app.get("/api/events")
async def api_events(limit: int = 300):
    return JSONResponse([asdict(e) for e in store.recent(limit)])


@app.get("/api/nodes")
async def api_nodes():
    return JSONResponse(
        [
            {
                "name": n.name,
                "role": n.role,
                "has_sip": n.has_sip,
                "sip_active": bool(subscribers.get(n.name) and subscribers[n.name].is_active),
                "sip_status": subscribers[n.name].status if n.name in subscribers else "pending",
            }
            for n in nodes
        ]
    )


@app.get("/events/stream")
async def events_stream():
    queue = store.subscribe()

    async def gen():
        try:
            while True:
                event = await queue.get()
                yield f"data: {json.dumps(asdict(event))}\n\n"
        finally:
            store.unsubscribe(queue)

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.post("/LogEvents")
async def receive_log_event(request: Request):
    """Matches i3-fe-core LoggingClient's POST target: {uri}/LogEvents."""
    body = await request.json()
    kind, summary, source, role, operation = _classify(body)
    # A node POSTing a LogEvent is proof it's alive right now — feed that into
    # liveness so an actively-working node shows green immediately, without
    # waiting on the next OPTIONS ping.
    sub = subscribers.get(source)
    if sub:
        sub.note_alive()
        package = _LOG_EVENT_TO_PACKAGE.get(body.get("logEventType"))
        if package:
            await sub.kick_initial_subscribe(package)
    store.add(kind, summary, body, source=source, role=role, operation=operation)
    return JSONResponse({"status": "accepted"}, status_code=201)


@app.post("/Reports")
async def receive_discrepancy_report(request: Request):
    """§3.7.1 DR web service: matches i3-fe-core's DiscrepancyReporting.submit()
    POST target, {resolutionUri-style base}/Reports (discrepancy/service.py).
    Passive receiver: accept, log, acknowledge — no resolution workflow, no
    reporter authorization/known-problem-service gating (this is a dev
    observability tool, not a responding FE)."""
    try:
        body = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return JSONResponse({"reason": "body is not a JSON object"}, status_code=454)
    if not isinstance(body, dict):
        return JSONResponse({"reason": "body is not a JSON object"}, status_code=454)

    missing = [k for k in _DR_REQUIRED_FIELDS if not body.get(k)]
    if missing:
        return JSONResponse(
            {"reason": f"missing MANDATORY field(s) {missing} (§3.7.1)"}, status_code=454
        )

    report_type = body.get("reportType", "Unknown")
    short_description = body.get("problemComments") or body.get("problemService") or ""

    reporting_agency = body.get("reportingAgencyName")
    node = _element_id_index.get(reporting_agency) if reporting_agency else None
    if node:
        source, role = node.name, node.role
    else:
        source, role = reporting_agency or "unknown", "?"

    # A node filing a DR is proof it's alive right now, same as /LogEvents.
    sub = subscribers.get(source)
    if sub:
        sub.note_alive()

    store.add(
        "dr_received",
        f"DR received: {report_type} — {short_description}",
        body,
        source=source,
        role=role,
    )

    response = {
        "respondingAgencyName": _DR_RESPONDING_AGENCY_NAME,
        "respondingContactJcard": _DR_RESPONDING_CONTACT_JCARD,
    }
    return JSONResponse(response, status_code=201)


@app.post("/api/clear")
async def clear_events():
    """Wipe the in-memory event buffer from the dashboard's Clear button."""
    store.clear()
    return JSONResponse({"status": "cleared"})


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "nodes_configured": len(nodes),
        "sip_subscribers_active": sum(1 for s in subscribers.values() if s.is_active),
        "events_buffered": len(store.recent(10_000)),
    }
