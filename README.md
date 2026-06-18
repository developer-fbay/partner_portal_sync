# Close CRM Sync Worker

A Python worker that syncs Close CRM leads and activities to Supabase on a recurring schedule. Designed to run on a DigitalOcean Droplet via Docker + cron.

## Features

- **Incremental sync** — polls for changes every few minutes using watermarks
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

# Partners only
docker run --rm --env-file .env close-sync-worker --phase partners
```

Check Supabase table `crm_sync_runs` for a row with `status = 'completed'`.

### 7. Schedule with cron

```bash
touch /var/log/close-sync.log
crontab -e
```

Add:

```cron
# Incremental sync every 5 minutes
*/5 * * * * docker run --rm --env-file /opt/close-sync-worker/.env close-sync-worker >> /var/log/close-sync.log 2>&1

# Full re-sync daily at 6:00 AM UTC
0 6 * * * docker run --rm --env-file /opt/close-sync-worker/.env close-sync-worker --mode full >> /var/log/close-sync.log 2>&1

# Partner activate/deactivate weekly (Mondays 7:00 AM UTC)
0 7 * * 1 docker run --rm --env-file /opt/close-sync-worker/.env close-sync-worker --phase partners >> /var/log/close-sync.log 2>&1
```

View logs:

```bash
tail -f /var/log/close-sync.log
```

### 8. Updating after code changes

```bash
cd /opt/close-sync-worker
git pull          # or scp updated files
docker build -t close-sync-worker .
```

Cron picks up the new image on the next run — no cron changes needed.

## Local development

```bash
# Create .env locally with the same variables as above (never commit it)
pip install -r requirements.txt
python main.py
python main.py --mode full
python main.py --phase partners --debug
```

## Sync phases

| Phase | What it does |
|-------|--------------|
| `all` (default) | partners → leads → lead_magnets → activities |
| `partners` | Sync partner active/inactive from Close + set `paid_partner` from dealsheet commission rows |
| `leads` | Sync leads from Lead Source smart view |
| `lead_magnets` | Sync LeadMaggy activities per lead |
| `activities` | Sync partner referrals and uploads |

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
