# NSW Property Tracker 🏠

Automated real estate data platform for the Ryde/Hunters Hill area (postcodes 2110–2115).

## Features

- **Multi-source scraping** — Domain.com.au API + realestate.com.au
- **Smart deduplication** — cross-source fuzzy address matching (≥92% similarity)
- **Bilingual dashboard** — English / 中文 toggle, localStorage persisted
- **Interactive filters** — postcode, suburb, price, beds, date range, days on market
- **Sortable columns** — client-side, no page reload
- **Export** — PDF (A4 landscape, color-coded rows) + CSV (UTF-8)
- **Telegram Bot** — @PropertyTrackerNSW_bot, rule-based, no AI
- **Daily worker** — systemd timer, 7 AM Sydney, Telegram summary notification

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  Residential IP (laptop)                            │
│  domain_scraper.py  ──►  POST /api/ingest           │
│  rea_scraper.py     ──►  POST /api/ingest           │
└──────────────────────────────┬──────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────┐
│  Server (172.16.1.110 / tmntech.ddns.net)           │
│  viewer/app.py  (Flask, port 8765)                  │
│  scraper/dedup.py  (SQLite deduplication)           │
│  worker.py  (systemd daily timer)                   │
│  telegram_bot.py  (polling bot)                     │
└─────────────────────────────────────────────────────┘
```

## Setup

### Server (auto-started via systemd)

```bash
# Services
systemctl status property-tracker          # web viewer
systemctl status property-tracker-bot      # telegram bot
systemctl status property-tracker-worker.timer  # daily scraper
```

### Scraper (run from residential IP)

```bash
pip install requests

# Domain API (needs Listings Management → Sandbox package activated)
python3 scraper/domain_scraper.py --push

# REA (run from home IP — datacenter IPs are blocked by REA)
python3 rea_push_client.py --push

# Both sources
python3 scraper/run_all.py --push

# Daily cron (add to crontab -e on your laptop)
0 7 * * * cd ~/tracker && python3 scraper/run_all.py --push
```

### Environment

```bash
# /etc/property-tracker.env
TELEGRAM_TOKEN=your_bot_token
TELEGRAM_CHAT=your_chat_id
INGEST_URL=http://localhost:8765/api/ingest
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Main dashboard |
| GET | `/api/stats` | JSON stats summary |
| GET | `/api/listings` | JSON listings (filterable) |
| POST | `/api/ingest` | Ingest scraped listings |

## Telegram Bot Commands

```
2110..2115    → listings by postcode
/all          → all active listings
/new          → listed this week
/stale        → 60+ days on market
/cheap        → lowest price first
/expensive    → highest price first
/stats        → summary counts
/ryde /hunters /gladesville /eastwood /meadowbank
/help         → full command list
```

## Target Postcodes

| Postcode | Suburbs |
|----------|---------|
| 2110 | Hunters Hill, Woolwich, Henley |
| 2111 | Gladesville, Meadowbank, West Ryde |
| 2112 | Ryde |
| 2113 | Eastwood, North Ryde |
| 2114 | Putney, Shepherd's Bay |
| 2115 | Ermington, Rydalmere |

## Tech Stack

- **Python 3** — Flask, SQLite, requests, Playwright
- **Systemd** — service + timer units
- **Telegram Bot API** — pure requests, no library
- **jsPDF + AutoTable** — client-side PDF generation
- **Vanilla JS** — i18n, sort, filters, CSV export

## Live

🌐 https://tmntech.ddns.net/tracker/
🤖 @PropertyTrackerNSW_bot
