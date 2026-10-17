# Fremantle biggest ship (TRMNL plugin)

Every 15 minutes a GitHub Action:

1. reads Fremantle Ports' public "Ships In Port" list (berth, type, last port, next port, next move),
2. listens to the free [aisstream.io](https://aisstream.io) feed for 7 minutes to get each ship's size and position,
3. picks the biggest commercial ship, guesses its cargo from type and berth,
4. posts the result to a TRMNL private plugin (webhook), which draws `trmnl/fremantle_quadrant.liquid`.

Ship sizes are remembered in `vessel_cache.json`, which the Action commits back when it learns a new one.

## Secrets (Settings → Secrets and variables → Actions)

| Name | Value |
|---|---|
| `AISSTREAM_API_KEY` | your aisstream.io key |
| `TRMNL_WEBHOOK_URL` | the plugin's webhook URL, `https://trmnl.com/api/custom_plugins/<uuid>` |

## Run locally

```bash
python3 -m venv ~/.venvs/trmnl && ~/.venvs/trmnl/bin/pip install -r requirements.txt
~/.venvs/trmnl/bin/python fremantle_port.py      # reads keys from .env next to the script
```

Data: Fremantle Ports VTMIS public pages (unofficial use of the data feed behind them, so it may change without notice) and aisstream.io.
