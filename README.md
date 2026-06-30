# Close CRM Sync Worker

A Python worker that syncs Close CRM leads and activities to Supabase on a recurring schedule. Designed to run on a DigitalOcean Droplet via Docker + cron.

## Features

- **Incremental sync** — polls for changes on a schedule using watermarks (default: every 12 hours)
- **Full re-sync** — complete refresh daily
- **Partner sync** — activates/deactivates partners from Close; sets `paid_partner` from dealsheet commission rows linked by lead ID, close lead ID, introducer, or company name
- **Idempotent upserts** — safe to retry, no duplicate data
- **Advisory locking** — prevents concurrent sync runs
- **Partner fuzzy matching** — resolves partner names to UUIDs

## What stays on the droplet

```
close-sync-worker/
├── main.py              # Entry point
├── sync.py              # Orchestrator
├── close_client.py
├── supabase_client.py
├── partner_matcher.py
├── mappers.py
├── config.py
├── requirements.txt
├── Dockerfile
└── .env                 # Secrets — create on server only, never commit
```

## DigitalOcean Droplet setup

### 1. Create the droplet

1. In [DigitalOcean](https://cloud.digitalocean.com/), create a **Droplet**.
2. **Image:** Ubuntu 24.04 LTS
3. **Size:** Basic $6/mo (1 GB RAM) is enough — this worker is lightweight
4. **Region:** Same region as your Supabase project if possible (e.g. London for `eu-west-2`)
5. Add your SSH key (or use a password, then switch to keys)

### 2. SSH in and install Docker

```bash
ssh root@YOUR_DROPLET_IP

apt update && apt upgrade -y
apt install -y docker.io git
systemctl enable --now docker
```

### 3. Deploy the app

**Option A — Git clone (if the repo is on GitHub):**

```bash
mkdir -p /opt/close-sync-worker
cd /opt/close-sync-worker
git clone https://github.com/YOUR_ORG/close-sync-worker.git .
```

**Option B — Copy files from your machine:**

```bash
# From your local machine (not on the droplet):
scp -r *.py requirements.txt Dockerfile root@YOUR_DROPLET_IP:/opt/close-sync-worker/
```
```

Then on the droplet:

```bash
mkdir -p /opt/close-sync-worker
cd /opt/close-sync-worker
```

### 4. Configure environment

Create `.env` on the droplet only (never commit it):

```bash
nano /opt/close-sync-worker/.env
```

Required variables:

| Variable | Where to find it |
|----------|------------------|
| `SUPABASE_URL` | Supabase → Project Settings → API |
| `SUPABASE_SERVICE_ROLE_KEY` | Same page (service role, not anon) |
| `SUPABASE_DB_PASSWORD` | Supabase → Project Settings → Database |
| `CLOSE_API_KEY` | Close → Settings → API |
| `CLOSE_LEAD_SOURCE_SMART_VIEW_ID` | Close smart view URL |

Optional: `CLOSE_PARTNERS_SMART_VIEW_ID`, `SUPABASE_POOLER_HOST`, `DATABASE_URL`, `PARTNER_MATCH_THRESHOLD`

Lock the file down:

```bash
chmod 600 .env
```

### 5. Build the Docker image

```bash
cd /opt/close-sync-worker
docker build -t close-sync-worker .
```

### 6. Test manually

```bash
# Incremental sync (all phases)
docker run --rm --env-file .env close-sync-worker

# Full re-sync
docker run --rm --env-file .env close-sync-worker --mode full

# Full leads backfill (two steps — run in order)
docker run --rm --env-file .env close-sync-worker --phase leads --mode full
docker run --rm --env-file .env close-sync-worker --phase lead_details --mode full

# Partners only
docker run --rm --env-file .env close-sync-worker --phase partners

# Full lead enrichment backfill (run after leads --mode full)
docker run --rm --env-file .env close-sync-worker --phase lead_details --mode full
```

Check Supabase table `crm_sync_runs` for a row with `status = 'completed'`.

### 7. Schedule with cron

```bash
touch /var/log/close-sync.log
crontab -e
```

Add:

```cron
# Incremental sync every 12 hours (08:00 and 20:00 UTC)
0 8,20 * * * /opt/close-sync-worker/scripts/docker-run.sh >> /var/log/close-sync.log 2>&1

# Full re-sync daily at 6:00 AM UTC
0 6 * * * /opt/close-sync-worker/scripts/docker-run.sh --mode full >> /var/log/close-sync.log 2>&1

# Partners activate/deactivate daily at midnight UTC
0 0 * * * /opt/close-sync-worker/scripts/docker-run.sh --phase partners >> /var/log/close-sync.log 2>&1

# Lead details enrichment daily at 1:00 AM UTC (staggered after partners)
0 1 * * * /opt/close-sync-worker/scripts/docker-run.sh --phase lead_details >> /var/log/close-sync.log 2>&1
```

View logs:

```bash
tail -f /var/log/close-sync.log
```

### 8. Updating after code changes

From your machine, push to GitHub:

```bash
git add -A
git commit -m "Your change description"
git push origin main
```

On the droplet, pull and rebuild:

```bash
ssh root@YOUR_DROPLET_IP
cd /opt/close-sync-worker
bash scripts/deploy-on-droplet.sh
```

Or run the steps manually:

```bash
cd /opt/close-sync-worker
git pull
docker build -t close-sync-worker .
```

Cron picks up the new image on the next run — no cron changes needed.

## Local development

```bash
# Create .env locally with the same variables as above (never commit it)
pip install -r requirements.txt
python main.py
python main.py --mode full
python main.py --phase leads --mode full
python main.py --phase lead_details --mode full
python main.py --phase partners --debug
```

## Sync phases

| Phase | What it does |
|-------|--------------|
| `all` (default) | partners → leads → lead_magnets → activities → dealsheet |
| `partners` | Sync partner active/inactive from Close + set `paid_partner` from dealsheet commission rows |
| `leads` | Sync leads from Lead Source smart view using **search API fields only** (fast — populates `close_lead_id` and core columns). Run this first. |
| `lead_details` | **Separate follow-up phase:** full `?_fields=_all` fetch per lead already in the `leads` table. Has its own watermark (`lead_details`). Run after `leads`. |
| `lead_magnets` | Sync latest LeadMaggy activity per lead from the **CA - LeadMaggy** smart view (`CLOSE_LEAD_MAGNET_SMART_VIEW_ID`, default `save_Fupn8b6Fn8TIPA5dT1dPXJrtYqruaZOYhM1dE4ZTRGh`). Supports both `LeadMaggy` and `LeadMaggy - Updated` activity types. |
| `activities` | Sync partner referrals (GEN1 + API), partner uploads (GEN2), and `custom_activities` parent rows |
| `dealsheet` | Sync Google Sheet dealsheet data |

## Database tables

| Table | Conflict key | Source |
|-------|--------------|--------|
| `leads` | `close_lead_id` | Smart view |
| `custom_activities` | `custom_activity_id` | Activities API |
| `partner_referral` | `custom_activity_uuid` | Activities API |
| `partner_upload` | `custom_activity_uuid` | Activities API |
| `lead_magnet` | `custom_activity_uuid` | Per-lead fetch |

Metadata: `crm_sync_runs`, `crm_sync_state`

## Troubleshooting

**Lock not acquired** — Another sync is running. Check `crm_sync_runs` for `status = 'running'`.

**Rate limited** — Worker backs off automatically. Reduce cron frequency if it persists.

**Missing data** — Check `crm_sync_runs.error_details` for the failed run.

**Docker permission denied** — Run docker as root, or add your user to the `docker` group.

**Advisory lock errors** — Ensure `SUPABASE_DB_PASSWORD` is set and you're using the session pooler on port **5432**, not the transaction pooler on 6543.
