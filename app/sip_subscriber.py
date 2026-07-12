"""Persistent SIP SUBSCRIBE/NOTIFY client (§2.4.1 / §2.4.2).

One instance per target node. Stays subscribed to that node's
emergency-ElementState and emergency-ServiceState event packages, refreshes
each subscription before it expires, ACKs every NOTIFY with a 200 OK, and
forwards every inbound NOTIFY/response into the shared EventStore tagged
with that node's name/role.

Wire format matches i3-fe-core's SIP notifier: raw SIP over UDP, NOTIFY body
is JSON (Content-Type Application/EmergencyCallData.*+json). Any FE built on
i3-fe-core (LVF, ECRF, MCS, GCS, MDS) speaks this the same way, so this class
is FE-agnostic — it never imports FE-specific code.
"""

from __future__ import annotations

import asyncio
import json
import logging
import socket
import time
import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.nodes import Node
    from app.store import EventStore

log = logging.getLogger("i3_admin_portal.sip")

SUBSCRIBE_EXPIRES = 300
REFRESH_MARGIN = 30
PACKAGES = ("emergency-ElementState", "emergency-ServiceState")

# Liveness has two independent channels, because state *changes* and node
# *death* are fundamentally different problems:
#
#   * State changes (Active→Overloaded→GoingDown, etc.) arrive as NOTIFYs —
#     a real-time push. The portal learns of them within milliseconds. That
#     part is pure §2.4 SUBSCRIBE/NOTIFY and needs no polling.
#   * Ungraceful death (crash, power-off, cut cable) can't be pushed — a dead
#     node can't announce it's dead. The only way to detect silence is to
#     expect a beat and notice its absence, so we actively probe.
#
# We probe with SIP OPTIONS (RFC 3261 §11) every WATCHDOG_INTERVAL_SECONDS —
# the canonical SIP liveness ping. Any live endpoint MUST answer, and unlike a
# re-SUBSCRIBE it doesn't disturb the subscription or trigger NOTIFY/log noise.
# Any inbound datagram (OPTIONS ack, NOTIFY, SUBSCRIBE 200) — plus a matching
# inbound LogEvent via note_alive() — counts as proof of life. A node is called
# unreachable only after WATCHDOG_INTERVAL_SECONDS * STALE_MULTIPLIER of total
# silence, so a couple of dropped UDP pings won't flap it. We also still request
# min-interval on the SUBSCRIBE (§2.4.1/§2.4.2's optional watchdog) as a bonus
# in case the FE honors it, but we no longer depend on it.
WATCHDOG_INTERVAL_SECONDS = 15
STALE_MULTIPLIER = 3  # tolerate a couple missed heartbeats before calling it unreachable

# The initial SUBSCRIBE after start() races container boot: if it's sent
# before the target node's SipWireAdapter has bound its socket, the datagram
# is silently dropped (UDP, no retransmit) and the normal refresh cadence
# wouldn't retry for up to SUBSCRIBE_EXPIRES - REFRESH_MARGIN seconds. So the
# initial SUBSCRIBE gets its own short-lived, backing-off retry loop, capped
# at INITIAL_SUBSCRIBE_RETRY_CAP seconds total, independent of _refresh_loop.
INITIAL_SUBSCRIBE_RETRY_START = 1.0
INITIAL_SUBSCRIBE_RETRY_MAX = 15.0
INITIAL_SUBSCRIBE_RETRY_CAP = 60.0


class _Protocol(asyncio.DatagramProtocol):
    def __init__(self, on_datagram):
        self._on_datagram = on_datagram

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self.transport = transport

    def datagram_received(self, data: bytes, addr) -> None:
        self._on_datagram(data, addr)

    def error_received(self, exc: Exception) -> None:
        log.warning("SIP UDP error: %s", exc)


def _header(lines: list[str], name: str) -> str:
    prefix = f"{name.lower()}:"
    return next((l for l in lines if l.lower().startswith(prefix)), "")


def _local_ip_for(host: str, port: int) -> str:
    """The local IP the OS would actually use to reach (host, port).

    socket.gethostbyname(socket.gethostname()) is unreliable on multi-homed
    boxes (VPN/WSL/Docker adapters) — it can return an address that can't
    route a reply back, silently breaking NOTIFY delivery. Connecting a UDP
    socket doesn't send any packets; it just asks the OS routing table which
    local interface it would use, which is what a Contact/Via header needs.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        try:
            s.connect((host, port))
            return s.getsockname()[0]
        except OSError:
            return "127.0.0.1"


class PersistentSipSubscriber:
    def __init__(self, node: "Node", store: "EventStore", local_port: int = 0) -> None:
        self._node = node
        self._store = store
        self._local_port = local_port
        self._transport: asyncio.DatagramTransport | None = None
        self._call_ids: dict[str, str] = {}
        self._cseq: dict[str, int] = {p: 0 for p in PACKAGES}
        self._refresh_tasks: dict[str, asyncio.Task] = {}
        self._initial_retry_tasks: dict[str, asyncio.Task] = {}
        self._initial_confirmed: dict[str, bool] = {p: False for p in PACKAGES}
        self._probe_task: asyncio.Task | None = None
        self._options_call_id = f"portal-opt-{uuid.uuid4().hex[:12]}"
        self._options_cseq = 0
        self._options_since_ack = 0  # OPTIONS pings sent with no ack — flags an FE that ignores OPTIONS
        self._options_warned = False
        self._local_ip = _local_ip_for(node.sip_host, node.sip_port)
        self.is_active = False
        self._last_heard_mono: float | None = None  # monotonic time of last proof-of-life

    @property
    def status(self) -> str:
        """'pending' — never heard from; 'live' — heard from recently
        (within the watchdog window); 'unreachable' — was heard from once
        but has gone quiet longer than the watchdog tolerates."""
        if self._last_heard_mono is None:
            return "pending"
        stale_after = WATCHDOG_INTERVAL_SECONDS * STALE_MULTIPLIER
        if time.monotonic() - self._last_heard_mono <= stale_after:
            return "live"
        return "unreachable"

    def note_alive(self) -> None:
        """Record proof of life from a channel other than this SIP socket.

        Called when an inbound LogEvent (§4.12.3) is attributed to this node:
        a node POSTing LogEvents is unambiguously alive and doing its job, even
        if its SIP notify path is momentarily quiet. This can only ever promote
        a node to 'live' — an idle node sends no LogEvents, so the OPTIONS probe
        remains the baseline that can call a node unreachable.
        """
        self._last_heard_mono = time.monotonic()

    def kick_initial_subscribe(self, package: str) -> None:
        """If the initial SUBSCRIBE for `package` hasn't been confirmed yet,
        immediately resend it now rather than waiting for the next scheduled
        retry tick. Safe to call redundantly — a confirmed package is a no-op,
        and an extra SUBSCRIBE while unconfirmed is harmless/idempotent."""
        if not self._initial_confirmed.get(package, True):
            self._subscribe(package, SUBSCRIBE_EXPIRES)

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        self._transport, _ = await loop.create_datagram_endpoint(
            lambda: _Protocol(self._on_datagram),
            local_addr=("0.0.0.0", self._local_port),
        )
        self._local_port = self._transport.get_extra_info("sockname")[1]
        self.is_active = True
        for package in PACKAGES:
            self._call_ids[package] = f"portal-{uuid.uuid4().hex[:12]}"
            self._subscribe(package, SUBSCRIBE_EXPIRES)
            self._refresh_tasks[package] = asyncio.create_task(self._refresh_loop(package))
            self._initial_retry_tasks[package] = asyncio.create_task(
                self._initial_subscribe_retry_loop(package)
            )
        self._probe_task = asyncio.create_task(self._probe_loop())

    async def stop(self) -> None:
        self.is_active = False
        if self._probe_task:
            self._probe_task.cancel()
        for task in self._refresh_tasks.values():
            task.cancel()
        for task in self._initial_retry_tasks.values():
            task.cancel()
        for package in PACKAGES:
            self._subscribe(package, 0)
        await asyncio.sleep(0.2)
        if self._transport:
            self._transport.close()

    async def _refresh_loop(self, package: str) -> None:
        try:
            while True:
                await asyncio.sleep(SUBSCRIBE_EXPIRES - REFRESH_MARGIN)
                self._subscribe(package, SUBSCRIBE_EXPIRES)
        except asyncio.CancelledError:
            pass

    async def _initial_subscribe_retry_loop(self, package: str) -> None:
        """Resend the initial SUBSCRIBE for `package` at an increasing
        interval (1s, 2s, 4s, 8s, capped at 15s) until a 200 OK matching its
        Call-ID is observed in _on_datagram, or INITIAL_SUBSCRIBE_RETRY_CAP
        seconds have elapsed. Covers the boot race where the first SUBSCRIBE
        is sent before the target's SIP listener is bound and is silently
        dropped over UDP. Runs only for the initial SUBSCRIBE — once
        confirmed, or once the cap is hit, _refresh_loop's normal cadence is
        the sole retry mechanism."""
        delay = INITIAL_SUBSCRIBE_RETRY_START
        elapsed = 0.0
        try:
            while elapsed < INITIAL_SUBSCRIBE_RETRY_CAP:
                await asyncio.sleep(delay)
                elapsed += delay
                if self._initial_confirmed[package]:
                    return
                self._subscribe(package, SUBSCRIBE_EXPIRES)
                delay = min(delay * 2, INITIAL_SUBSCRIBE_RETRY_MAX)
            if not self._initial_confirmed[package]:
                log.warning(
                    "%s: no confirmed SUBSCRIBE response for %s after ~%.0fs of "
                    "initial retries — falling back to the standard %ds refresh cycle",
                    self._node.name, package, elapsed, SUBSCRIBE_EXPIRES,
                )
        except asyncio.CancelledError:
            pass

    async def _probe_loop(self) -> None:
        """Watchdog: ping with SIP OPTIONS every WATCHDOG_INTERVAL_SECONDS so a
        node that has gone silent is detected without waiting on the 270s
        SUBSCRIBE refresh. Probe immediately, then on the interval."""
        try:
            while True:
                self._send_options()
                await asyncio.sleep(WATCHDOG_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            pass

    def _send_options(self) -> None:
        if not self._transport:
            return
        host, port = self._node.sip_host, self._node.sip_port
        self._options_cseq += 1
        tag = uuid.uuid4().hex[:8]
        branch = f"z9hG4bK{uuid.uuid4().hex[:16]}"
        msg = (
            f"OPTIONS sip:{self._node.name}@{host}:{port} SIP/2.0\r\n"
            f"Via: SIP/2.0/UDP {self._local_ip}:{self._local_port};branch={branch}\r\n"
            f"From: <sip:i3-admin-portal@{self._local_ip}>;tag={tag}\r\n"
            f"To: <sip:{self._node.name}@{host}>\r\n"
            f"Call-ID: {self._options_call_id}\r\n"
            f"CSeq: {self._options_cseq} OPTIONS\r\n"
            f"Contact: <sip:i3-admin-portal@{self._local_ip}:{self._local_port}>\r\n"
            f"Max-Forwards: 70\r\n"
            f"Content-Length: 0\r\n\r\n"
        )
        self._transport.sendto(msg.encode(), (host, port))
        self._options_since_ack += 1
        log.debug("OPTIONS ping -> %s (%s:%s) cseq=%s", self._node.name, host, port, self._options_cseq)
        if self._options_since_ack >= STALE_MULTIPLIER and not self._options_warned:
            self._options_warned = True
            log.warning(
                "%s has not answered %d OPTIONS watchdog pings — this FE likely doesn't "
                "respond to SIP OPTIONS, so liveness will fall back to NOTIFY/LogEvent traffic only.",
                self._node.name, self._options_since_ack,
            )

    def _add(self, kind: str, summary: str, detail: dict) -> None:
        self._store.add(kind, summary, detail, source=self._node.name, role=self._node.role)

    def _subscribe(self, package: str, expires: int) -> None:
        if not self._transport:
            return
        host, port = self._node.sip_host, self._node.sip_port
        self._cseq[package] += 1
        call_id = self._call_ids[package]
        tag = uuid.uuid4().hex[:8]
        branch = f"z9hG4bK{uuid.uuid4().hex[:16]}"

        # RFC 4661 / RFC 6446 event rate control: the minimum notification
        # interval is a filter document in the SUBSCRIBE body, not an Event
        # header parameter. A rate filter with <min-interval> asks the
        # notifier to space NOTIFYs at least this far apart (the §2.4
        # watchdog mechanism). Omitted on unsubscribe (expires == 0), which
        # carries no body.
        body = ""
        extra_headers = ""
        if expires > 0:
            body = (
                '<?xml version="1.0" encoding="UTF-8"?>\r\n'
                '<filter-set xmlns="urn:ietf:params:xml:ns:simple-filter">\r\n'
                f'  <filter id="rate-{package}">\r\n'
                '    <what>\r\n'
                f'      <min-interval>{WATCHDOG_INTERVAL_SECONDS}</min-interval>\r\n'
                '    </what>\r\n'
                '  </filter>\r\n'
                '</filter-set>\r\n'
            )
            extra_headers = "Content-Type: application/simple-filter+xml\r\n"

        msg = (
            f"SUBSCRIBE sip:{self._node.name}@{host}:{port} SIP/2.0\r\n"
            f"Via: SIP/2.0/UDP {self._local_ip}:{self._local_port};branch={branch}\r\n"
            f"From: <sip:i3-admin-portal@{self._local_ip}>;tag={tag}\r\n"
            f"To: <sip:{self._node.name}@{host}>\r\n"
            f"Call-ID: {call_id}\r\n"
            f"CSeq: {self._cseq[package]} SUBSCRIBE\r\n"
            f"Contact: <sip:i3-admin-portal@{self._local_ip}:{self._local_port}>\r\n"
            f"Event: {package}\r\n"
            f"Accept: Application/EmergencyCallData.{'ElementState' if 'ElementState' in package else 'ServiceState'}+json\r\n"
            f"Max-Forwards: 70\r\n"
            f"Expires: {expires}\r\n"
            f"{extra_headers}"
            f"Content-Length: {len(body.encode())}\r\n\r\n"
            f"{body}"
        )
        self._transport.sendto(msg.encode(), (host, port))
        action = "unsubscribe" if expires == 0 else ("refresh" if self._cseq[package] > 1 else "subscribe")
        self._add(
            "sip_subscribe",
            f"{action} {package} (Expires={expires})",
            {"package": package, "expires": expires, "callId": call_id, "action": action},
        )

    def _on_datagram(self, data: bytes, addr) -> None:
        text = data.decode(errors="replace")
        head, _, body = text.partition("\r\n\r\n")
        lines = head.split("\r\n")
        first_line = lines[0]
        self._last_heard_mono = time.monotonic()

        if first_line.startswith("SIP/2.0"):
            if "OPTIONS" in _header(lines, "CSeq").upper():
                # watchdog ping ack — liveness already updated above, don't log noise
                if self._options_warned:
                    log.info("%s is now answering OPTIONS watchdog pings again", self._node.name)
                self._options_since_ack = 0
                self._options_warned = False
                return
            min_expires = _header(lines, "Min-Expires")
            if "200" in first_line:
                resp_call_id = _header(lines, "Call-ID").split(":", 1)[-1].strip()
                for package, call_id in self._call_ids.items():
                    if call_id == resp_call_id:
                        self._initial_confirmed[package] = True
                        break
            self._add(
                "sip_subscribe",
                f"response: {first_line}" + (f" ({min_expires})" if min_expires else ""),
                {"status": first_line, "from": f"{addr[0]}:{addr[1]}"},
            )
            return

        if first_line.startswith("NOTIFY"):
            event_pkg = _header(lines, "Event").split(":", 1)[-1].strip() or "unknown"
            sub_state = _header(lines, "Subscription-State").split(":", 1)[-1].strip()

            # RFC 6665 §4.4.1: only accept a NOTIFY that belongs to a
            # subscription dialog we actually created. Match on Call-ID
            # against the Call-IDs we used when subscribing. A NOTIFY whose
            # Call-ID we don't recognise gets 481 (Subscription Does Not
            # Exist), not a 200 — this is what a conformant subscriber does
            # and prevents this tool from silently blessing stray NOTIFYs.
            notify_call_id = _header(lines, "Call-ID").split(":", 1)[-1].strip()
            known_call_ids = set(self._call_ids.values())
            if notify_call_id not in known_call_ids:
                log.warning(
                    "%s: NOTIFY with unknown Call-ID %r (not one of our "
                    "subscriptions) — responding 481",
                    self._node.name, notify_call_id,
                )
                self._add(
                    "sip_notify",
                    f"NOTIFY {event_pkg} — REJECTED 481 (unknown subscription)",
                    {"package": event_pkg, "callId": notify_call_id, "rejected": "481"},
                )
                self._respond_notify(lines, addr, "481 Subscription Does Not Exist")
                return

            parsed_body: object = body
            try:
                parsed_body = json.loads(body) if body.strip() else {}
            except json.JSONDecodeError:
                pass
            self._add(
                "sip_notify",
                f"NOTIFY {event_pkg} ({sub_state})",
                {"package": event_pkg, "subscriptionState": sub_state, "body": parsed_body},
            )
            self._respond_notify(lines, addr, "200 OK")

    def _respond_notify(self, request_lines: list[str], addr, status_line: str) -> None:
        """Send a SIP response to an inbound NOTIFY. status_line is the text
        after 'SIP/2.0 ', e.g. '200 OK' or '481 Subscription Does Not Exist'."""
        if not self._transport:
            return
        resp = (
            f"SIP/2.0 {status_line}\r\n"
            f"{_header(request_lines, 'Via')}\r\n"
            f"{_header(request_lines, 'From')}\r\n"
            f"{_header(request_lines, 'To')}\r\n"
            f"{_header(request_lines, 'Call-ID')}\r\n"
            f"{_header(request_lines, 'CSeq')}\r\n"
            "Content-Length: 0\r\n\r\n"
        )
        self._transport.sendto(resp.encode(), addr)
