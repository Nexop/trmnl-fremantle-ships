#!/usr/bin/env python3
"""
Fremantle Ships: latest arrival in the Port of Fremantle, for a TRMNL plugin.

Each run:
1. Reads Fremantle Ports' public VTMIS lists:
   - "Ships In Port" (berth, ship type, last port, next port, next planned move)
   - "Today's Movements" (every arrival / shift / departure today, with actual times)
2. Records each completed commercial *Arrival* (first arrival of a visit) in arrivals.json,
   so the latest arrival survives the midnight reset of the port's list.
3. Listens to aisstream.io over Fremantle for a few minutes for ship sizes, draught and
   speed; sizes are remembered in vessel_cache.json.
4. Features the most recent arrival that is still in port (falls back to the biggest ship
   if no arrival has been recorded yet), with the biggest ship in port as a footnote.
5. Writes latest.json and, if TRMNL_WEBHOOK_URL is set, posts it to the TRMNL plugin.

Settings come from environment variables, or a .env file next to this script:
    AISSTREAM_API_KEY=...
    TRMNL_WEBHOOK_URL=https://trmnl.com/api/custom_plugins/<uuid>   (optional)

Usage:
    python fremantle_port.py           # 7 minutes of AIS
    python fremantle_port.py 3         # 3 minutes
    python fremantle_port.py 0         # port data only, no AIS
"""
from __future__ import annotations

import asyncio
import http.cookiejar
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import shannon_listen as ais  # same folder: AIS listener + vessel parsing

HERE = Path(__file__).resolve().parent
CACHE_PATH = HERE / "vessel_cache.json"     # ship sizes remembered between runs
ARRIVALS_PATH = HERE / "arrivals.json"      # recent arrivals remembered between runs
BASE = "https://www3.fremantleports.com.au/VTMIS/"
PERTH = timezone(timedelta(hours=8))
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129 Safari/537.36"

# Report codes on the public VTMIS dashboard
REPORTS = {
    "ships_in_port": "FMP-WEB-0004",
    "todays_movements": "FMP-WEB-2028",     # today's arrivals/removals/departures with start/end times
    "completed_movements": "FMP-WEB-0002",  # completed today only (subset of the above)
    "expected_movements": "FMP-WEB-0001",
    "cruise_ships": "FMP-WEB-0003",
}

COMMERCIAL = {"BULK CARRIER", "TANKER", "CONTAINER SHIP", "VEHICLES CARRIER",
              "GENERAL CARGO", "PASSENGER SHIP", "HEAVY LIFT"}
KEEP_ARRIVALS = 60   # how many recent arrivals to remember


# ---------------------------------------------------------------- housekeeping
def load_env() -> None:
    """Read KEY=value lines from .env next to this script (real environment variables win)."""
    env_file = HERE / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def prune_logs(days: float = 2) -> None:
    """Delete this script's own raw AIS logs older than `days`."""
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


def ms_of(v) -> int | None:
    """'/Date(1792350600000+0800)/' -> 1792350600000 (epoch ms)."""
    m = re.search(r"Date\((-?\d+)", str(v or ""))
    return int(m.group(1)) if m else None


def fmt_time(ms: int | None, fmt: str = "%a %d %b %H:%M") -> str:
    return datetime.fromtimestamp(ms / 1000, PERTH).strftime(fmt) if ms else ""


def ms_date(v) -> str:
    return fmt_time(ms_of(v))


def ago_text(ms: int | None, now_ms: int) -> str:
    if not ms:
        return ""
    mins = max(0, (now_ms - ms) // 60000)
    if mins < 60:
        return f"{mins} min ago"
    hours = mins // 60
    if hours < 24:
        return f"{hours} h ago"
    days = hours // 24
    return "1 day ago" if days == 1 else f"{days} days ago"


# ---------------------------------------------------------------- cargo, status
def cargo_guess(vclass: str, berth: str, to_loc: str) -> str:
    where = f"{berth} {to_loc}".upper()
    vclass = (vclass or "").upper()
    # Kept short so they fit on one line in a quadrant.
    if "GRAIN" in where:
        return "Grain"                 # CBH Kwinana Grain Terminal
    if "O.R.J" in where or "OIL REFINERY" in where:
        return "Fuel"                  # Kwinana fuel import terminal
    if "ALCOA" in where:
        return "Alumina"               # Alcoa Kwinana (also caustic soda)
    if "CONTAINER" in where or vclass == "CONTAINER SHIP":
        return "Containers"
    if vclass == "VEHICLES CARRIER":
        return "Vehicles"
    if vclass == "PASSENGER SHIP":
        return "Passengers"
    if vclass == "TANKER":
        return "Liquid bulk"
    if vclass == "BULK CARRIER":
        return "Dry bulk"
    if vclass == "HEAVY LIFT":
        return "Heavy lift"
    if vclass == "GENERAL CARGO":
        return "General cargo"
    return ""


def norm(name: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", (name or "").upper())


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


# ---------------------------------------------------------------- memory files
def _read_json(path: Path, default):
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def load_cache() -> dict:
    cache = _read_json(CACHE_PATH, None)
    if cache is not None:
        return cache
    # First run: rebuild sizes from any earlier Fremantle logs in test_runs/
    replay: dict[int, ais.Vessel] = {}
    for f in sorted((HERE / "test_runs").glob("raw_fremantle*.jsonl")):
        for line in f.open():
            try:
                ais.handle(json.loads(line), replay, {})
            except (json.JSONDecodeError, KeyError, TypeError):
                continue
    cache = {}
    if replay:
        update_cache(cache, replay)
    return cache


def update_cache(cache: dict, vessels: dict) -> int:
    """Store size/type for every vessel whose static data we heard. Returns count new or changed."""
    n = 0
    now = datetime.now(PERTH).strftime("%Y-%m-%d %H:%M")
    for v in vessels.values():
        if not v.has_static or not v.length:
            continue
        entry = {"name": v.name, "imo": v.imo, "type_code": v.type_code,
                 "length_m": v.length, "beam_m": v.beam, "draught_m": v.draught,
                 "destination": v.destination}
        old = {k: x for k, x in cache.get(str(v.mmsi), {}).items() if k != "updated"}
        if old != entry:                      # only touch the file when something changed
            cache[str(v.mmsi)] = {**entry, "updated": now}
            n += 1
    if n or not CACHE_PATH.exists():
        CACHE_PATH.write_text(json.dumps(cache, indent=1, sort_keys=True) + "\n")
    return n


def update_arrivals(movements: list[dict]) -> tuple[list[dict], int]:
    """Add today's completed commercial arrivals to arrivals.json. Returns (arrivals newest first, new count)."""
    known = {a["visit"]: a for a in _read_json(ARRIVALS_PATH, [])}
    new = 0
    for m in movements:
        if (m.get("JOB_TYPE_NAME") or "").strip().lower() != "arrival":
            continue
        if (m.get("STATUS_TYPE_NAME") or "").strip().lower() != "completed":
            continue
        if (m.get("VESSEL_CLASS") or "").strip() not in COMMERCIAL:
            continue
        visit = str(m.get("VISIT_NUMBER") or m.get("VESSEL_NAME"))
        t = ms_of(m.get("END_TIME")) or ms_of(m.get("START_TIME"))
        if not t or visit in known:
            continue
        known[visit] = {"visit": visit, "name": (m.get("VESSEL_NAME") or "").strip(),
                        "class": (m.get("VESSEL_CLASS") or "").strip(), "time_ms": t,
                        "to": (m.get("TO_LOCATION_NAME") or "").strip(),
                        "last_port": (m.get("LASTPORT_NAME") or "").strip()}
        new += 1
    arrivals = sorted(known.values(), key=lambda a: a["time_ms"], reverse=True)[:KEEP_ARRIVALS]
    if new or not ARRIVALS_PATH.exists():
        ARRIVALS_PATH.write_text(json.dumps(arrivals, indent=1) + "\n")
    return arrivals, new


# ---------------------------------------------------------------- main
def main() -> None:
    minutes = float(sys.argv[1]) if len(sys.argv) > 1 else 7  # > the ~6 min static-data cycle
    load_env()
    now_ms = int(time.time() * 1000)

    print("Fetching Fremantle Ports lists...")
    opener, stamp = port_session()
    in_port = port_report(opener, stamp, REPORTS["ships_in_port"])
    try:
        movements = port_report(opener, stamp, REPORTS["todays_movements"])
    except Exception as e:  # arrivals are a bonus; never let them stop the run
        print(f"  couldn't read today's movements ({e}); carrying on")
        movements = []
    arrivals, new_arrivals = update_arrivals(movements)

    # A ship working two berths appears twice; keep the first.
    seen, ships = set(), []
    for r in in_port:
        key = r.get("VISIT_NUMBER") or r.get("SHIP")
        if key not in seen:
            seen.add(key)
            ships.append(r)
    commercial = [r for r in ships if (r.get("VESSEL_CLASS") or "").strip() in COMMERCIAL]
    print(f"  {len(ships)} vessels in port, {len(commercial)} commercial ships; "
          f"{len(movements)} movements today, {new_arrivals} new arrivals recorded "
          f"({len(arrivals)} remembered)\n")

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
            "visit": str(r.get("VISIT_NUMBER") or ""),
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
            "arrived_at": "",
            "arrived_ago": "",
        })

    by_size = sorted(rows, key=lambda x: ((x["length_m"] or 0) * (x["beam_m"] or 0), x["length_m"] or 0), reverse=True)
    biggest = by_size[0] if by_size and by_size[0]["length_m"] else None

    # Latest arrival that is still in port
    rows_by_visit = {x["visit"]: x for x in rows}
    featured, mode = None, "biggest"
    for a in arrivals:
        if a["visit"] in rows_by_visit:
            featured, mode = rows_by_visit[a["visit"]], "arrival"
            featured["arrived_at"] = fmt_time(a["time_ms"], "%a %H:%M")
            featured["arrived_ago"] = ago_text(a["time_ms"], now_ms)
            break
    if featured is None:
        featured = biggest

    print(f"{'Ship':<20}{'Type':<17}{'LxB m':<9}{'Drgt':<6}{'Where':<30}{'From':<14}{'Next':<12}")
    print("-" * 108)
    for x in by_size:
        lxb = f"{x['length_m']}x{x['beam_m']}" if x["length_m"] else "?"
        print(f"{x['name'][:19]:<20}{x['type'][:16]:<17}{lxb:<9}{str(x['draught_m'] or ''):<6}"
              f"{x['berth'][:29]:<30}{x['last_port'][:13]:<14}{x['next_port'][:11]:<12}")

    unsized = [x["name"] for x in by_size if not x["length_m"]]
    footnote = None
    if biggest and featured and biggest["visit"] != featured["visit"]:
        footnote = {"name": biggest["name"], "type": biggest["type"], "length_m": biggest["length_m"]}

    payload = {
        "updated": datetime.now(PERTH).strftime("%a %d %b %H:%M"),
        "mode": mode,                 # "arrival" = latest arrival; "biggest" = fallback
        "ships_in_port": len(commercial),
        "unsized": unsized,
        "ship": featured,             # the ship shown on screen
        "biggest": footnote,          # biggest ship in port, if it isn't the featured one
    }
    (HERE / "latest.json").write_text(json.dumps(payload, indent=2))

    if featured:
        label = "Latest arrival" if mode == "arrival" else "Biggest (no arrival recorded yet)"
        size = f"{featured['length_m']} m" if featured["length_m"] else "size pending"
        when = f", arrived {featured['arrived_at']} ({featured['arrived_ago']})" if mode == "arrival" else ""
        print(f"\n{label}: {featured['name']} - {featured['type']}, {size}{when} - "
              f"{featured['status'].lower()} at {featured['berth']} - from {featured['last_port']}, "
              f"next {featured['next_port']} - cargo: {featured['cargo_guess'] or 'unknown'}")
        if footnote:
            print(f"Biggest in port: {footnote['name']}, {footnote['length_m']} m")
    else:
        print("\nNo ship to show yet (no recorded arrival and no sized ship).")
    print("Wrote latest.json")
    push_to_trmnl(payload)


if __name__ == "__main__":
    main()
