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
        self._probe_task: asyncio.Task | None = None
        self._options_call_id = f"portal-opt-{uuid.uuid4().hex[:12]}"
        self._options_cseq = 0
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
        self._probe_task = asyncio.create_task(self._probe_loop())

    async def stop(self) -> None:
        self.is_active = False
        if self._probe_task:
            self._probe_task.cancel()
        for task in self._refresh_tasks.values():
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
        msg = (
            f"SUBSCRIBE sip:{self._node.name}@{host}:{port} SIP/2.0\r\n"
            f"Via: SIP/2.0/UDP {self._local_ip}:{self._local_port};branch={branch}\r\n"
            f"From: <sip:i3-admin-portal@{self._local_ip}>;tag={tag}\r\n"
            f"To: <sip:{self._node.name}@{host}>\r\n"
            f"Call-ID: {call_id}\r\n"
            f"CSeq: {self._cseq[package]} SUBSCRIBE\r\n"
            f"Contact: <sip:i3-admin-portal@{self._local_ip}:{self._local_port}>\r\n"
            f"Event: {package};min-interval={WATCHDOG_INTERVAL_SECONDS}\r\n"
            f"Max-Forwards: 70\r\n"
            f"Expires: {expires}\r\n"
            f"Content-Length: 0\r\n\r\n"
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
                return  # watchdog ping ack — liveness already updated above, don't log noise
            min_expires = _header(lines, "Min-Expires")
            self._add(
                "sip_subscribe",
                f"response: {first_line}" + (f" ({min_expires})" if min_expires else ""),
                {"status": first_line, "from": f"{addr[0]}:{addr[1]}"},
            )
            return

        if first_line.startswith("NOTIFY"):
            event_pkg = _header(lines, "Event").split(":", 1)[-1].strip() or "unknown"
            sub_state = _header(lines, "Subscription-State").split(":", 1)[-1].strip()
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
            self._ack_notify(lines, addr)

    def _ack_notify(self, request_lines: list[str], addr) -> None:
        if not self._transport:
            return
        resp = (
            "SIP/2.0 200 OK\r\n"
            f"{_header(request_lines, 'Via')}\r\n"
            f"{_header(request_lines, 'From')}\r\n"
            f"{_header(request_lines, 'To')}\r\n"
            f"{_header(request_lines, 'Call-ID')}\r\n"
            f"{_header(request_lines, 'CSeq')}\r\n"
            "Content-Length: 0\r\n\r\n"
        )
        self._transport.sendto(resp.encode(), addr)
