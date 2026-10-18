#!/usr/bin/env python3
"""
Fremantle biggest-ship prototype.

1. Pulls the official "Ships In Port" list from Fremantle Ports' public VTMIS page
   (berth, ship type, last port, next port, planned departure).
2. Listens to aisstream.io over Fremantle for a few minutes to get each ship's
   size, draught, position and speed.
3. Matches the two by ship name, picks the biggest commercial ship, guesses its
   cargo from type + berth, and writes latest.json.
4. If TRMNL_WEBHOOK_URL is set, posts latest.json to the TRMNL private plugin.

Settings come from environment variables, or a .env file next to this script:
    AISSTREAM_API_KEY=...
    TRMNL_WEBHOOK_URL=https://trmnl.com/api/custom_plugins/<uuid>   (optional)

Usage:
    python fremantle_port.py           # 7 minutes of AIS (sizes are cached in vessel_cache.json)
    python fremantle_port.py 8         # 8 minutes
    python fremantle_port.py 0         # port data only, no AIS
"""
from __future__ import annotations

import asyncio
import http.cookiejar
import json
import os
import re
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import shannon_listen as ais  # same folder: AIS listener + vessel parsing

HERE = Path(__file__).resolve().parent
CACHE_PATH = HERE / "vessel_cache.json"   # ship sizes remembered between runs


def load_env() -> None:
    """Read KEY=value lines from .env next to this script (real environment variables win)."""
    env_file = HERE / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def prune_logs(days: float = 2) -> None:
    """Delete this script's own raw AIS logs older than `days` (they add up when run every 15 min)."""
    import time
    cutoff = time.time() - days * 86400
    for f in (HERE / "test_runs").glob("raw_fremantle_port_*.jsonl"):
        if f.stat().st_mtime < cutoff:
            f.unlink(missing_ok=True)


def push_to_trmnl(payload: dict) -> None:
    url = os.environ.get("TRMNL_WEBHOOK_URL", "").strip()
    if not url:
        print("No TRMNL_WEBHOOK_URL set, so nothing sent to TRMNL.")
        return
    body = json.dumps({"merge_variables": payload}, separators=(",", ":")).encode()
    if len(body) > 5000:
        sys.exit(f"Payload is {len(body)} bytes; TRMNL's webhook limit is 5 KB.")
    req = urllib.request.Request(url, data=body, method="POST",
                                 headers={"Content-Type": "application/json", "User-Agent": "fremantle-ships"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            print(f"Sent to TRMNL ({len(body)} bytes): HTTP {r.status}")
    except urllib.error.HTTPError as e:
        hint = {403: "check the plugin uses the Webhook strategy and your account has the Developer add-on",
                422: "payload too large or malformed",
                429: "rate limit hit (12 updates an hour on the standard plan)"}.get(e.code, "")
        sys.exit(f"TRMNL rejected the update: HTTP {e.code}. {hint}")
BASE = "https://www3.fremantleports.com.au/VTMIS/"
PERTH = timezone(timedelta(hours=8))
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129 Safari/537.36"

# Report codes on the public VTMIS dashboard
REPORTS = {
    "ships_in_port": "FMP-WEB-0004",
    "expected_movements": "FMP-WEB-0001",
    "cruise_ships": "FMP-WEB-0003",
}

COMMERCIAL = {"BULK CARRIER", "TANKER", "CONTAINER SHIP", "VEHICLES CARRIER",
              "GENERAL CARGO", "PASSENGER SHIP", "HEAVY LIFT"}


# ---------------------------------------------------------------- port data
def port_session():
    jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    opener.addheaders = [("User-Agent", UA)]
    html = opener.open(BASE + "dashb.ashx?db=fmp.public&btn=ShipsInPort", timeout=30).read().decode("utf-8", "replace")
    m = re.search(r"__stamp = '([^']+)'", html)
    if not m:
        raise RuntimeError("Couldn't find the session stamp on the Fremantle Ports page (layout changed?)")
    return opener, m.group(1) + "\v" + "fmp.public/main-view"


def port_report(opener, stamp: str, code: str) -> list[dict]:
    body = {"request": {"requestID": f"py-{code}", "reportCode": code, "dataSource": None,
                        "filterName": None, "parameters": [], "metaVersion": 0,
                        "_type": "TGetDataXREQ:#WebX.Services", "stamp": stamp}}
    req = urllib.request.Request(BASE + "services/wxdata.svc/GetDataX",
                                 data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json; charset=utf-8",
                                          "X-Requested-With": "XMLHttpRequest",
                                          "Referer": BASE + "dashb.ashx?db=fmp.public&btn=ShipsInPort"})
    data = json.loads(opener.open(req, timeout=30).read())
    table = data["d"]["Tables"][0]
    cols = [c["Name"] for c in table["MetaData"]["Columns"]]
    return [dict(zip(cols, row)) for row in table["Data"]]


def ms_date(v) -> str:
    """'/Date(1792350600000+0800)/' -> '19 Oct 03:10' Perth time."""
    m = re.search(r"Date\((-?\d+)", str(v or ""))
    if not m:
        return ""
    return datetime.fromtimestamp(int(m.group(1)) / 1000, PERTH).strftime("%a %d %b %H:%M")


# ---------------------------------------------------------------- cargo guess
def cargo_guess(vclass: str, berth: str, to_loc: str) -> str:
    where = f"{berth} {to_loc}".upper()
    vclass = (vclass or "").upper()
    if "GRAIN" in where:
        return "Grain (CBH Kwinana Grain Terminal)"
    if "O.R.J" in where or "OIL REFINERY" in where:
        return "Fuel products (Kwinana fuel import terminal)"
    if "ALCOA" in where:
        return "Alumina / caustic soda (Alcoa Kwinana)"
    if "CONTAINER" in where or vclass == "CONTAINER SHIP":
        return "Containers"
    if vclass == "VEHICLES CARRIER":
        return "Cars, trucks and machinery"
    if vclass == "PASSENGER SHIP":
        return "Passengers"
    if vclass == "TANKER":
        return "Liquid bulk"
    if vclass == "BULK CARRIER":
        return "Dry bulk" + (" (Kwinana Bulk Terminal)" if "KWIN BULK" in where else "")
    if vclass == "HEAVY LIFT":
        return "Project / heavy-lift cargo"
    if vclass == "GENERAL CARGO":
        return "General cargo"
    return ""


def norm(name: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", (name or "").upper())


def load_cache() -> dict:
    try:
        return json.loads(CACHE_PATH.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    # First run: rebuild sizes from any earlier Fremantle logs in test_runs/
    replay: dict[int, ais.Vessel] = {}
    for f in sorted((HERE / "test_runs").glob("raw_fremantle*.jsonl")):
        for line in f.open():
            try:
                ais.handle(json.loads(line), replay, {})
            except (json.JSONDecodeError, KeyError, TypeError):
                continue
    cache: dict = {}
    if replay:
        update_cache(cache, replay)
    return cache


def update_cache(cache: dict, vessels: dict) -> int:
    """Store size/type for every vessel whose static data we heard. Returns count added or refreshed."""
    n = 0
    now = datetime.now(PERTH).strftime("%Y-%m-%d %H:%M")
    for v in vessels.values():
        if not v.has_static or not v.length:
            continue
        entry = {
            "name": v.name, "imo": v.imo, "type_code": v.type_code,
            "length_m": v.length, "beam_m": v.beam, "draught_m": v.draught,
            "destination": v.destination,
        }
        old = {k: x for k, x in cache.get(str(v.mmsi), {}).items() if k != "updated"}
        if old != entry:                      # only touch the file when something changed
            cache[str(v.mmsi)] = {**entry, "updated": now}
            n += 1
    if n or not CACHE_PATH.exists():
        CACHE_PATH.write_text(json.dumps(cache, indent=1, sort_keys=True) + "\n")
    return n


def phase_of(row: dict, v) -> str:
    """Where to draw the ship on the route line: arriving | in_port | departing."""
    next_to = (row.get("TO_LOCATION_NAME") or "").strip().upper()
    moving = v is not None and isinstance(v.sog, (int, float)) and v.sog >= 2
    if moving:
        return "departing" if next_to == "SEA" else "arriving"
    if (row.get("LOCATION_TYPE") or "").lower() == "anchorage" and next_to not in ("", "SEA"):
        return "arriving"   # waiting at anchor for a berth
    return "in_port"


def status_of(row: dict, v) -> str:
    if v is not None and isinstance(v.sog, (int, float)) and v.sog >= 2:
        return f"Under way, {v.sog:.0f} kn"
    loc_type = (row.get("LOCATION_TYPE") or "").lower()
    return "At anchor" if loc_type == "anchorage" else "Alongside"


# ---------------------------------------------------------------- main
def main() -> None:
    minutes = float(sys.argv[1]) if len(sys.argv) > 1 else 7  # > the ~6 min static-data cycle
    load_env()

    print("Fetching Fremantle Ports 'Ships In Port'...")
    opener, stamp = port_session()
    in_port = port_report(opener, stamp, REPORTS["ships_in_port"])
    # A ship working two berths appears twice; keep the first.
    seen, ships = set(), []
    for r in in_port:
        key = r.get("VISIT_NUMBER") or r.get("SHIP")
        if key not in seen:
            seen.add(key)
            ships.append(r)
    commercial = [r for r in ships if (r.get("VESSEL_CLASS") or "").strip() in COMMERCIAL]
    print(f"  {len(ships)} vessels in port, {len(commercial)} commercial ships\n")

    vessels: dict[int, ais.Vessel] = {}
    if minutes > 0:
        key = ais.load_api_key()
        ais.BBOX = ais.BOXES["fremantle"]
        out_dir = HERE / "test_runs"
        out_dir.mkdir(exist_ok=True)
        prune_logs()
        stamp_utc = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
        raw_path = out_dir / f"raw_fremantle_port_{stamp_utc}.jsonl"
        print(f"Listening to AIS over Fremantle for {minutes:g} min...")
        try:
            asyncio.run(ais.listen(minutes, key, raw_path, vessels, {}))
        except KeyboardInterrupt:
            print("\nStopped early.")
        print(f"  heard {len(vessels)} vessels, {sum(v.has_static for v in vessels.values())} with size data\n")

    cache = load_cache()
    added = update_cache(cache, vessels)
    print(f"Size cache: {len(cache)} ships known ({added} new or changed this run)\n")

    by_name = {norm(v.name): v for v in vessels.values() if v.name}
    cache_by_name = {norm(c["name"]): c for c in cache.values() if c.get("name")}

    rows = []
    for r in commercial:
        key = norm(r.get("SHIP"))
        v = by_name.get(key)                      # heard live this run (position, speed)
        c = cache.get(str(v.mmsi)) if v else None  # size by MMSI if we know it
        c = c or cache_by_name.get(key)            # otherwise size by name
        rows.append({
            "name": (r.get("SHIP") or "").strip(),
            "type": (r.get("VESSEL_CLASS") or "").strip().title(),
            "berth": (r.get("BERTH_ANCHORAGE") or "").strip(),
            "status": status_of(r, v),
            "phase": phase_of(r, v),
            "last_port": (r.get("LASTPORT_NAME") or "").strip(),
            "next_port": (r.get("NEXTPORT_NAME") or "").strip(),
            "next_move": ms_date(r.get("DEPARTURE")),
            "next_move_to": (r.get("TO_LOCATION_NAME") or "").strip(),
            "cargo_guess": cargo_guess(r.get("VESSEL_CLASS"), r.get("BERTH_ANCHORAGE"), r.get("TO_LOCATION_NAME")),
            "length_m": (v.length if v and v.length else None) or (c or {}).get("length_m"),
            "beam_m": (v.beam if v and v.beam else None) or (c or {}).get("beam_m"),
            "draught_m": (v.draught if v and v.draught else None) or (c or {}).get("draught_m"),
            "ais_destination": (v.destination if v and v.destination else "") or (c or {}).get("destination", ""),
            "lat": v.lat if v else None,
            "lon": v.lon if v else None,
            "mmsi": v.mmsi if v else None,
        })

    rows.sort(key=lambda x: ((x["length_m"] or 0) * (x["beam_m"] or 0), x["length_m"] or 0), reverse=True)

    print(f"{'Ship':<20}{'Type':<17}{'LxB m':<9}{'Drgt':<6}{'Where':<30}{'From':<14}{'Next':<12}")
    print("-" * 108)
    for x in rows:
        lxb = f"{x['length_m']}x{x['beam_m']}" if x["length_m"] else "?"
        print(f"{x['name'][:19]:<20}{x['type'][:16]:<17}{lxb:<9}{str(x['draught_m'] or ''):<6}"
              f"{x['berth'][:29]:<30}{x['last_port'][:13]:<14}{x['next_port'][:11]:<12}")
    matched = sum(1 for x in rows if x["length_m"])
    print(f"\nAIS size found for {matched} of {len(rows)} commercial ships.")

    biggest = rows[0] if rows and rows[0]["length_m"] else None
    unsized = [x["name"] for x in rows if not x["length_m"]]
    payload = {
        "updated": datetime.now(PERTH).strftime("%a %d %b %H:%M"),
        "ships_in_port": len(commercial),
        "unsized": unsized,   # commercial ships whose size we haven't heard yet
        "biggest": biggest,
        "others": [{k: x[k] for k in ("name", "type", "length_m", "berth", "status")} for x in rows[1:5]],
    }
    (HERE / "latest.json").write_text(json.dumps(payload, indent=2))
    if biggest:
        print(f"\nBiggest: {biggest['name']} - {biggest['type']}, {biggest['length_m']} m - "
              f"{biggest['status'].lower()} at {biggest['berth']} - from {biggest['last_port']}, "
              f"next {biggest['next_port']} - cargo: {biggest['cargo_guess'] or 'unknown'}")
    else:
        print("\nNo AIS sizes matched, so 'biggest' can't be chosen yet (port list still saved).")
    if unsized:
        print(f"Not yet sized ({len(unsized)}): {', '.join(unsized)}")
    print("Wrote latest.json")
    push_to_trmnl(payload)


if __name__ == "__main__":
    main()
