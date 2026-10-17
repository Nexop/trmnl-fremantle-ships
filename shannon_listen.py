#!/usr/bin/env python3
"""
Shannon Estuary AIS coverage test.

Listens to aisstream.io for a set number of minutes over the Shannon Estuary,
tracks every vessel heard, and at the end prints a table (biggest first) and
saves:
  - snapshot_<UTC>.csv   one row per vessel
  - raw_<UTC>.jsonl      every message received (for building the plugin later)

API key: set AISSTREAM_API_KEY in your environment, or put a line
    AISSTREAM_API_KEY=your_key_here
in a file called .env next to this script.

Usage:
    python shannon_listen.py            # 10 minutes
    python shannon_listen.py 20         # 20 minutes
    python shannon_listen.py 5 ireland  # other boxes: shannon, ireland, dover, fremantle
"""
from __future__ import annotations

import asyncio
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import websockets
except ImportError:
    sys.exit("Missing library: run  pip install websockets  (inside your venv)")

HERE = Path(__file__).resolve().parent
URL = "wss://stream.aisstream.io/v0/stream"

# aisstream wants [[lat, lon], [lat, lon]] corners.
BOXES = {
    # Shannon Estuary: Kerry Head / Loop Head mouth east to Limerick city.
    "shannon": [[52.38, -10.00], [52.72, -8.55]],
    # All Irish coastal waters (stops short of Wales/Scotland).
    "ireland": [[51.20, -11.00], [55.60, -5.40]],
    # Dover Strait: one of the busiest lanes in the world - a sanity check.
    "dover": [[50.80, 1.00], [51.20, 1.90]],
    # Fremantle: Inner Harbour, Gage Roads, Owen Anchorage, Cockburn Sound, outer anchorages
    # and the "Outside Port Limits" waiting area north/west of Rottnest.
    "fremantle": [[-32.40, 115.20], [-31.60, 115.85]],
}
BBOX = BOXES["shannon"]

# Rough port areas, used to say where vessels were heard on wider runs.
AREAS = {
    "Shannon Estuary": (52.38, -10.00, 52.72, -8.55),
    "Dublin Bay": (53.20, -6.30, 53.45, -5.90),
    "Belfast Lough / NI": (54.40, -6.10, 55.40, -5.40),
    "Cork Harbour": (51.70, -8.45, 51.95, -8.10),
    "Waterford / Rosslare": (52.10, -7.20, 52.35, -6.20),
    "Galway Bay": (53.05, -9.70, 53.30, -8.95),
    "Donegal / Killybegs": (54.40, -8.80, 55.40, -7.20),
}


def area_of(lat, lon) -> str:
    if lat is None or lon is None:
        return "?"
    for name, (la1, lo1, la2, lo2) in AREAS.items():
        if la1 <= lat <= la2 and lo1 <= lon <= lo2:
            return name
    return "Other / at sea"

SHIP_TYPES = [
    (20, 29, "Wing-in-ground"), (30, 30, "Fishing"), (31, 32, "Towing"),
    (33, 33, "Dredger"), (34, 34, "Diving ops"), (35, 35, "Military"),
    (36, 36, "Sailing"), (37, 37, "Pleasure craft"), (40, 49, "High-speed craft"),
    (50, 50, "Pilot"), (51, 51, "Search & rescue"), (52, 52, "Tug"),
    (53, 53, "Port tender"), (54, 54, "Anti-pollution"), (55, 55, "Law enforcement"),
    (58, 58, "Medical"), (60, 69, "Passenger"), (70, 79, "Cargo"),
    (80, 89, "Tanker"), (90, 99, "Other"),
]
NAV_STATUS = {
    0: "Under way (engine)", 1: "At anchor", 2: "Not under command",
    3: "Restricted manoeuvrability", 4: "Constrained by draught", 5: "Moored",
    6: "Aground", 7: "Fishing", 8: "Under way (sail)", 15: "Undefined",
}


def ship_type_name(code: int | None) -> str:
    if not code:
        return ""
    for lo, hi, name in SHIP_TYPES:
        if lo <= code <= hi:
            return name
    return f"Type {code}"


def load_api_key() -> str:
    key = os.environ.get("AISSTREAM_API_KEY", "").strip()
    env_file = HERE / ".env"
    if not key and env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.strip().startswith("AISSTREAM_API_KEY="):
                key = line.split("=", 1)[1].strip().strip('"').strip("'")
    if not key:
        sys.exit("No API key. Set AISSTREAM_API_KEY or add it to .env next to this script.")
    return key


class Vessel:
    def __init__(self, mmsi: int):
        self.mmsi = mmsi
        self.name = ""
        self.imo = None
        self.callsign = ""
        self.type_code = None
        self.length = None
        self.beam = None
        self.draught = None
        self.destination = ""
        self.eta = ""
        self.lat = None
        self.lon = None
        self.sog = None
        self.cog = None
        self.heading = None
        self.nav_status = None
        self.msgs = 0
        self.has_static = False
        self.first_seen = None
        self.last_seen = None

    def area(self) -> float:
        return (self.length or 0) * (self.beam or 0)

    def row(self) -> dict:
        return {
            "mmsi": self.mmsi,
            "name": self.name,
            "imo": self.imo or "",
            "callsign": self.callsign,
            "type": ship_type_name(self.type_code),
            "type_code": self.type_code or "",
            "length_m": self.length or "",
            "beam_m": self.beam or "",
            "draught_m": self.draught or "",
            "destination": self.destination,
            "eta": self.eta,
            "nav_status": NAV_STATUS.get(self.nav_status, "") if self.nav_status is not None else "",
            "sog_kn": self.sog if self.sog is not None else "",
            "cog": self.cog if self.cog is not None else "",
            "lat": self.lat if self.lat is not None else "",
            "lon": self.lon if self.lon is not None else "",
            "msgs": self.msgs,
            "has_static": self.has_static,
            "first_seen_utc": self.first_seen or "",
            "last_seen_utc": self.last_seen or "",
        }


def handle(msg: dict, vessels: dict[int, Vessel], type_counts: dict[str, int]) -> Vessel | None:
    mtype = msg.get("MessageType", "?")
    type_counts[mtype] = type_counts.get(mtype, 0) + 1
    meta = msg.get("MetaData") or {}
    body = (msg.get("Message") or {}).get(mtype) or {}

    mmsi = meta.get("MMSI") or body.get("UserID")
    if not mmsi:
        return None
    is_new = mmsi not in vessels
    v = vessels.setdefault(mmsi, Vessel(mmsi))
    v.msgs += 1
    ts = meta.get("time_utc") or datetime.now(timezone.utc).isoformat()
    v.first_seen = v.first_seen or ts
    v.last_seen = ts

    name = (meta.get("ShipName") or "").strip()
    if name:
        v.name = name
    lat, lon = meta.get("latitude"), meta.get("longitude")
    if lat is not None and lon is not None:
        v.lat, v.lon = round(lat, 5), round(lon, 5)

    if mtype in ("PositionReport", "StandardClassBPositionReport", "ExtendedClassBPositionReport"):
        v.sog = body.get("Sog", v.sog)
        v.cog = body.get("Cog", v.cog)
        hdg = body.get("TrueHeading")
        if hdg is not None and hdg != 511:
            v.heading = hdg
        if "NavigationalStatus" in body:
            v.nav_status = body["NavigationalStatus"]

    elif mtype == "ShipStaticData":
        v.has_static = True
        v.name = (body.get("Name") or v.name).strip()
        v.imo = body.get("ImoNumber") or v.imo
        v.callsign = (body.get("CallSign") or v.callsign).strip()
        v.type_code = body.get("Type") or v.type_code
        dim = body.get("Dimension") or {}
        length = (dim.get("A") or 0) + (dim.get("B") or 0)
        beam = (dim.get("C") or 0) + (dim.get("D") or 0)
        v.length = length or v.length
        v.beam = beam or v.beam
        v.draught = body.get("MaximumStaticDraught") or v.draught
        v.destination = (body.get("Destination") or v.destination).strip()
        eta = body.get("Eta") or {}
        if eta.get("Month"):
            v.eta = f"{eta['Day']:02d}/{eta['Month']:02d} {eta['Hour']:02d}:{eta['Minute']:02d} UTC"

    elif mtype == "StaticDataReport":  # class B static (small craft)
        rep = body.get("ReportA") or {}
        if rep.get("Valid") and rep.get("Name"):
            v.name = rep["Name"].strip()
        rep_b = body.get("ReportB") or {}
        if rep_b.get("Valid"):
            v.type_code = rep_b.get("ShipType") or v.type_code
            dim = rep_b.get("Dimension") or {}
            length = (dim.get("A") or 0) + (dim.get("B") or 0)
            beam = (dim.get("C") or 0) + (dim.get("D") or 0)
            v.length = length or v.length
            v.beam = beam or v.beam

    return v if is_new else None


async def listen(minutes: float, key: str, raw_path: Path,
                 vessels: dict[int, Vessel], type_counts: dict[str, int]) -> None:
    sub = {"APIKey": key, "BoundingBoxes": [BBOX]}
    deadline = time.monotonic() + minutes * 60
    with raw_path.open("w", buffering=1) as raw:
        while time.monotonic() < deadline:
            try:
                async with websockets.connect(URL, max_size=None) as ws:
                    await ws.send(json.dumps(sub))
                    print("Connected. Listening...\n")
                    while True:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            return
                        try:
                            data = await asyncio.wait_for(ws.recv(), timeout=remaining)
                        except asyncio.TimeoutError:
                            return
                        msg = json.loads(data)
                        if "error" in msg:
                            sys.exit(f"aisstream error: {msg['error']}")
                        raw.write(json.dumps(msg) + "\n")
                        new = handle(msg, vessels, type_counts)
                        if new:
                            mins = (minutes * 60 - remaining) / 60
                            print(f"  +{mins:4.1f} min  new vessel  {new.mmsi}  {new.name or '(name not yet heard)'}")
            except (OSError, websockets.exceptions.WebSocketException) as e:
                print(f"Connection dropped ({e}); reconnecting in 5 s...")
                await asyncio.sleep(5)


def report(vessels: dict[int, Vessel], type_counts: dict[str, int], minutes: float, csv_path: Path) -> None:
    ordered = sorted(vessels.values(), key=lambda v: (v.area(), v.length or 0), reverse=True)
    rows = [v.row() for v in ordered]
    if rows:
        with csv_path.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)

    total = sum(type_counts.values())
    print("\n" + "=" * 100)
    print(f"Listened {minutes:g} min | {total} messages | {len(vessels)} vessels | "
          f"{sum(v.has_static for v in vessels.values())} with ship details")
    print("Message types: " + ", ".join(f"{k} {n}" for k, n in sorted(type_counts.items(), key=lambda x: -x[1])))
    print("=" * 100)
    if not ordered:
        print("No vessels heard. Either the area is empty or there's no receiver coverage here.")
        return
    hdr = f"{'Name':<22}{'Type':<16}{'LxB m':<10}{'Drgt':<6}{'Destination':<20}{'Status':<22}{'Kn':>5}"
    print(hdr)
    print("-" * len(hdr))
    for v in ordered:
        lxb = f"{v.length}x{v.beam}" if v.length else "?"
        status = NAV_STATUS.get(v.nav_status, "") if v.nav_status is not None else ""
        sog = f"{v.sog:.1f}" if isinstance(v.sog, (int, float)) else ""
        print(f"{(v.name or str(v.mmsi))[:21]:<22}{ship_type_name(v.type_code)[:15]:<16}{lxb:<10}"
              f"{str(v.draught or ''):<6}{v.destination[:19]:<20}{status[:21]:<22}{sog:>5}")
    areas: dict[str, int] = {}
    for v in ordered:
        a = area_of(v.lat, v.lon)
        areas[a] = areas.get(a, 0) + 1
    print("\nVessels by area: " + ", ".join(f"{k} {n}" for k, n in sorted(areas.items(), key=lambda x: -x[1])))
    print(f"\nSaved: test_runs/{csv_path.name}")


def main() -> None:
    global BBOX
    minutes = float(sys.argv[1]) if len(sys.argv) > 1 else 10
    box_name = sys.argv[2].lower() if len(sys.argv) > 2 else "shannon"
    if box_name not in BOXES:
        sys.exit(f"Unknown box '{box_name}'. Choose from: {', '.join(BOXES)}")
    BBOX = BOXES[box_name]
    key = load_api_key()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    out_dir = HERE / "test_runs"
    out_dir.mkdir(exist_ok=True)
    raw_path = out_dir / f"raw_{box_name}_{stamp}.jsonl"
    csv_path = out_dir / f"snapshot_{box_name}_{stamp}.csv"
    vessels: dict[int, Vessel] = {}
    type_counts: dict[str, int] = {}

    print(f"AIS test: {box_name}, {minutes:g} min, box {BBOX}")
    try:
        asyncio.run(listen(minutes, key, raw_path, vessels, type_counts))
    except KeyboardInterrupt:
        print("\nStopped early.")
    report(vessels, type_counts, minutes, csv_path)
    print(f"Raw messages: test_runs/{raw_path.name}")


if __name__ == "__main__":
    main()
