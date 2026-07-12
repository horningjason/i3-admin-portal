# i3 Admin Portal

A local observability dashboard for **multiple** i3 front-end nodes at
once — any mix of LVF, ECRF, MCS, GCS, MDS, since they all share
`i3-fe-core`'s logging and SIP notifier shape. It does **not** modify or
depend on the internals of `lvf-service` or `i3-fe-core` — it only speaks
the two wire protocols those repos already expose:

1. **LogEvent receiver** (§4.12.3.1.2) — accepts the same `POST {uri}/LogEvents`
   JSON payload an FE's `LoggingClient` sends, and shows every query,
   response, state change, DR, and subscribe log live, attributed to the
   sending node.
2. **Persistent SIP subscriber** (§2.4.1 / §2.4.2), **one instance per
   configured node** — stays SUBSCRIBEd to each node's
   `emergency-ElementState` and `emergency-ServiceState` event packages over
   raw UDP, refreshing before expiry, and shows every NOTIFY live.

Both feeds land on one page (`/`) via Server-Sent Events, split into a
LogEvents panel and a SIP panel, with a node status strip, click-to-expand
raw JSON, and toggle chips to filter by event kind and by source node.

## Run

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
copy nodes.json.example nodes.json    # then edit to your actual nodes
uvicorn app.main:app --reload --port 9100
```

Open `http://localhost:9100`.

## Configure nodes (`nodes.json`)

One entry per FE instance you want watched:

```json
{ "name": "lvf-child", "role": "LVF", "sip_host": "192.168.1.10", "sip_port": 5060, "element_id": "lvf-child.nd911.nd.gov" }
```

- `name` / `role` — free text, shown in the dashboard's node strip and
  source filter. `role` groups nodes by FE type (LVF/ECRF/MCS/GCS/MDS/…).
- `sip_host` / `sip_port` — omit both to skip SIP subscription for that
  node (LogEvents attribution still works if `element_id` is set).
- `element_id` — optional. Must match that node's `ElementIdentity.element_id`
  (whatever it stamps into `LogEvent.elementId`). When set, inbound
  LogEvents from that node are attributed to `name`/`role` instead of
  showing the raw elementId with an unknown role.

`PORTAL_NODES_FILE` (default `nodes.json`) picks the config file path.
Missing/invalid file → no SIP watching, but the LogEvents receiver still
works and attributes by raw `elementId`/`agencyId`.

## Point a node at it

In that node's env (`.env` or docker-compose environment) — the variable
name is LVF's; other FEs built on `i3-fe-core` will have their own but the
target and payload are identical:

```
LVF_LOGGING_SERVICE_URI=http://localhost:9100
```

Posts unsigned JSON today (no JWS signer configured on any node yet — see
LVF's `CLAUDE.md`, `requiredAlgorithms: []`), which this receiver accepts
as-is.

## Scope

This is a local dev/ops tool: single process, in-memory ring buffer (2000
events), no auth, no persistence across restarts. It exists to make i3 FE
interfaces (logging, SIP notify, and eventually DR/metrics) observable in
one place across a whole node fleet while testing topology, without
touching `lvf-service` or `i3-fe-core`.

## Roadmap

- [x] LogEvent receiver + live feed
- [x] Persistent SIP subscriber (ElementState/ServiceState) + live feed
- [x] Multi-node config, per-node SIP subscriber, source/role attribution + filtering
- [ ] Discrepancy Reporting peer (receive reports filed via `LVF_DR_ENDPOINT`)
- [ ] Prometheus `/metrics` scrape + summary tiles per node
