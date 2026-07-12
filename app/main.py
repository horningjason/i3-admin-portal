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


def _classify(body: dict) -> tuple[str, str, str, str]:
    """Returns (kind, summary, source, role). Source/role come from matching
    the LogEvent's elementId against nodes.json; unregistered senders still
    show up, just tagged with their raw elementId/agencyId and role '?'."""
    event_type = body.get("logEventType", "Unknown")
    direction = body.get("direction", "")
    kind = _LOG_EVENT_KIND.get(event_type, "log_other")

    element_id = body.get("elementId")
    node = _element_id_index.get(element_id) if element_id else None
    if node:
        source, role = node.name, node.role
    else:
        source, role = element_id or body.get("agencyId") or "unknown", "?"

    if event_type == "LostQueryLogEvent":
        summary = f"{direction} query {body.get('queryId', '')}"
    elif event_type == "LostResponseLogEvent":
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
    return kind, summary, source, role


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
    kind, summary, source, role = _classify(body)
    store.add(kind, summary, body, source=source, role=role)
    return JSONResponse({"status": "accepted"}, status_code=201)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "nodes_configured": len(nodes),
        "sip_subscribers_active": sum(1 for s in subscribers.values() if s.is_active),
        "events_buffered": len(store.recent(10_000)),
    }
