# Resporg News Refresh — Schedule Documentation

The resporg news enrichment can be re-run periodically to pick up new
developments. Recommended cadence:

| Tier | Cadence | Approx cost | Notes |
|---|---|---|---|
| Major carriers (Tier 1, ~30 resporgs) | Quarterly | ~$0.40 | Bandwidth, Verizon, AT&T, Comcast, T-Mobile, etc. — frequent news |
| Mid-tier with website (~250 resporgs) | Semi-annually | ~$3.00 | Most of the production set |
| Long tail (Tier 3, no Sanity category) | Skip | — | Sonar Pro reflects back tollfreenumbers.com |
| Industry deep-dive (6 queries) | Quarterly | ~$0.10 | Pair with each cycle's published article |

## How to run a refresh

The enrichment script is resume-safe — it skips already-processed IDs.
To force a full re-run, delete `data/resporg_news_enrichment.json` first.

```bash
cd /var/www/resporgs.com   # or wherever the repo lives

# Production resporg refresh (full)
python scripts/sonar_enrich_resporgs.py
python scripts/sonar_push_to_sanity.py --apply

# Industry articles refresh
python scripts/sonar_industry_deepdive.py
python scripts/save_article_drafts.py
python scripts/push_article_drafts.py --apply

# Refresh local from Sanity + restart
python scripts/fetch_sanity_docs.py
sudo systemctl restart resporgs
```

## Cron entry — quarterly tier-1 refresh

To run on the 1st of January, April, July, October at 06:00 UTC:

```cron
0 6 1 1,4,7,10 *  cd /var/www/resporgs.com && \
  /usr/bin/python scripts/sonar_enrich_resporgs.py >> logs/sonar.log 2>&1 && \
  /usr/bin/python scripts/sonar_push_to_sanity.py --apply >> logs/sonar.log 2>&1 && \
  /usr/bin/python scripts/fetch_sanity_docs.py >> logs/sonar.log 2>&1 && \
  systemctl restart resporgs
```

## systemd timer alternative (preferred)

`/etc/systemd/system/resporgs-news-refresh.service`:
```ini
[Unit]
Description=Resporgs.com news enrichment refresh
After=network-online.target

[Service]
Type=oneshot
WorkingDirectory=/var/www/resporgs.com
ExecStart=/usr/bin/python scripts/sonar_enrich_resporgs.py
ExecStart=/usr/bin/python scripts/sonar_push_to_sanity.py --apply
ExecStart=/usr/bin/python scripts/fetch_sanity_docs.py
ExecStart=/bin/systemctl restart resporgs
StandardOutput=append:/var/log/resporgs/news-refresh.log
StandardError=append:/var/log/resporgs/news-refresh.log
```

`/etc/systemd/system/resporgs-news-refresh.timer`:
```ini
[Unit]
Description=Quarterly news refresh

[Timer]
OnCalendar=*-01,04,07,10-01 06:00:00 UTC
Persistent=true

[Install]
WantedBy=timers.target
```

Enable:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now resporgs-news-refresh.timer
sudo systemctl list-timers resporgs-news-refresh.timer
```

## Environment requirements

`apikey.env` (or environment) must contain:
- `SANITY_API_TOKEN` — for mutate API access
- `OPENROUTER_API_KEY` — for Sonar Pro (in `local-prospector/.env` on Bill's machine; copy to production env)
