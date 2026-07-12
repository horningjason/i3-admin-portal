"""Multi-node target config.

Loads the list of i3 FE instances this portal watches — any mix of LVF,
ECRF, MCS, GCS, MDS nodes, since they all share i3-fe-core's SIP notifier
and LogEvent shape. One JSON file, one entry per node.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger("i3_admin_portal.nodes")


@dataclass(frozen=True)
class Node:
    name: str
    role: str = "?"
    sip_host: str | None = None
    sip_port: int | None = None
    element_id: str | None = None

    @property
    def has_sip(self) -> bool:
        return bool(self.sip_host and self.sip_port)


def load_nodes(path: str | Path) -> list[Node]:
    p = Path(path)
    if not p.exists():
        log.warning("Nodes file %s not found — no SIP subscriptions, LogEvents still accepted", p)
        return []
    try:
        raw = json.loads(p.read_text())
    except (json.JSONDecodeError, OSError):
        log.exception("Failed to read/parse nodes file %s — no nodes loaded", p)
        return []

    nodes = []
    for entry in raw:
        if "name" not in entry:
            log.warning("Skipping nodes.json entry missing 'name': %r", entry)
            continue
        nodes.append(
            Node(
                name=entry["name"],
                role=entry.get("role", "?"),
                sip_host=entry.get("sip_host"),
                sip_port=entry.get("sip_port"),
                element_id=entry.get("element_id"),
            )
        )
    return nodes
