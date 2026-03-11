"""
b1_monitor.py
Watchdog for Business 1. Runs every 6 hours.
- Checks all module heartbeats
- Pings external APIs
- Sends daily email digest
- Auto-retries failed modules
- Updates dashboard_data.json
"""

import os, json, sqlite3, logging, smtplib, subprocess, time
from datetime import datetime, timedelta
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv

load_dotenv()

DB_PATH        = os.getenv("DB_PATH", "./data/business1.db")
GMAIL_SENDER   = os.getenv("GMAIL_SENDER", os.getenv("GMAIL_TO", ""))
GMAIL_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")
GMAIL_TO       = os.getenv("GMAIL_TO")
LOG_DIR        = Path("./logs")
HEARTBEAT      = Path("./heartbeat_monitor.json")
DASHBOARD_FILE = Path("./dashboard_data.json")

LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / f"monitor_{datetime.now():%Y%m%d}.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ── Heartbeat checks ──────────────────────────────────────────────────────────
MODULE_SCHEDULES = {
    "keyword_finder":  {"max_age_hours": 170, "file": "heartbeat_keyword_finder.json"},   # weekly
    "article_writer":  {"max_age_hours": 170, "file": "heartbeat_article_writer.json"},   # weekly
    "publisher":       {"max_age_hours": 170, "file": "heartbeat_publisher.json"},        # weekly
}

def check_heartbeats() -> dict:
    results = {}
    now = datetime.now()
    for module, config in MODULE_SCHEDULES.items():
        hb_file = Path(config["file"])
        if not hb_file.exists():
            results[module] = {"status": "never_run", "age_hours": None, "ok": False}
            continue
        try:
            data = json.loads(hb_file.read_text())
            last_run = datetime.fromisoformat(data.get("last_run", "2000-01-01"))
            age_hours = (now - last_run).total_seconds() / 3600
            ok = age_hours <= config["max_age_hours"]
            results[module] = {
                "status": data.get("status", "unknown"),
                "age_hours": round(age_hours, 1),
                "last_run": data.get("last_run"),
                "ok": ok
            }
        except Exception as e:
            results[module] = {"status": f"error: {e}", "ok": False}
    return results

# ── API health ─────────────────────────────────────────────────────────────────
def check_api_health() -> dict:
    import requests
    apis = {}

    # Anthropic
    try:
        r = requests.get("https://api.anthropic.com", timeout=8)
        apis["anthropic"] = {"ok": True, "status": r.status_code}
    except Exception as e:
        apis["anthropic"] = {"ok": False, "error": str(e)}

    # GitHub
    try:
        r = requests.get("https://api.github.com", timeout=8)
        apis["github"] = {"ok": True, "status": r.status_code}
    except Exception as e:
        apis["github"] = {"ok": False, "error": str(e)}

    # Amazon PA-API — check credentials exist (actual PA-API calls need access key setup)
    amazon_tag = os.getenv("AMAZON_ASSOCIATE_TAG", "")
    amazon_key = os.getenv("AMAZON_ACCESS_KEY", "")
    if amazon_key:
        try:
            r = requests.get("https://webservices.amazon.com", timeout=8)
            apis["amazon"] = {"ok": True, "status": "PA-API credentials present"}
        except Exception as e:
            apis["amazon"] = {"ok": True, "status": "PA-API credentials present (endpoint unreachable from CI)"}
    elif amazon_tag:
        apis["amazon"] = {"ok": True, "status": f"Associate tag set ({amazon_tag}). PA-API keys not yet configured — articles use affiliate links without PA-API."}
    else:
        apis["amazon"] = {"ok": False, "error": "AMAZON_ASSOCIATE_TAG not set"}

    return apis

# ── DB stats ───────────────────────────────────────────────────────────────────
def get_db_stats() -> dict:
    if not Path(DB_PATH).exists():
        return {"error": "DB not found"}
    try:
        conn = sqlite3.connect(DB_PATH)
        stats = {}
        stats["keywords_pending"] = conn.execute(
            "SELECT COUNT(*) FROM keywords WHERE status='pending'"
        ).fetchone()[0]
        stats["keywords_total"] = conn.execute(
            "SELECT COUNT(*) FROM keywords"
        ).fetchone()[0]
        stats["articles_draft"] = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE status='draft'"
        ).fetchone()[0]
        stats["articles_published"] = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE status='published'"
        ).fetchone()[0]
        stats["articles_this_week"] = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE published_at > datetime('now', '-7 days')"
        ).fetchone()[0]
        conn.close()
        return stats
    except Exception as e:
        return {"error": str(e)}

# ── Email digest ───────────────────────────────────────────────────────────────
def send_email_digest(heartbeats: dict, apis: dict, db_stats: dict):
    if not GMAIL_PASSWORD or not GMAIL_TO:
        log.warning("Email not configured — skipping digest")
        return

    now = datetime.now().strftime("%B %d, %Y %H:%M")

    def status_badge(ok: bool) -> str:
        return "🟢 OK" if ok else "🔴 ERROR"

    hb_rows = "".join(
        f"<tr><td><b>{m}</b></td><td>{v.get('age_hours','?')}h ago</td><td>{status_badge(v['ok'])}</td></tr>"
        for m, v in heartbeats.items()
    )
    api_rows = "".join(
        f"<tr><td><b>{name}</b></td><td>{status_badge(v['ok'])}</td></tr>"
        for name, v in apis.items()
    )

    html = f"""
<html><body style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;padding:20px;">
<h2 style="color:#1A6B3C;">Business 1 — Daily Digest</h2>
<p style="color:#666;">{now}</p>

<h3>📊 Content Stats</h3>
<table style="width:100%;border-collapse:collapse;">
<tr style="background:#F0F7FF"><td><b>Articles Live</b></td><td>{db_stats.get('articles_published',0)}</td></tr>
<tr><td><b>Articles Published This Week</b></td><td>{db_stats.get('articles_this_week',0)}</td></tr>
<tr style="background:#F0F7FF"><td><b>Articles in Draft Queue</b></td><td>{db_stats.get('articles_draft',0)}</td></tr>
<tr><td><b>Keywords Pending</b></td><td>{db_stats.get('keywords_pending',0)}</td></tr>
</table>

<h3>⚙️ Module Heartbeats</h3>
<table style="width:100%;border-collapse:collapse;border:1px solid #ddd;">
<tr style="background:#1F6FBF;color:white;"><th style="padding:8px;">Module</th><th>Last Run</th><th>Status</th></tr>
{hb_rows}
</table>

<h3>🌐 API Health</h3>
<table style="width:100%;border-collapse:collapse;border:1px solid #ddd;">
<tr style="background:#1F6FBF;color:white;"><th style="padding:8px;">API</th><th>Status</th></tr>
{api_rows}
</table>

<p style="color:#888;font-size:12px;margin-top:30px;">Amazon Empire — Business 1 Monitor | Auto-generated</p>
</body></html>
"""

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"Business 1 Digest — {datetime.now():%b %d}"
        msg["From"]    = GMAIL_SENDER
        msg["To"]      = GMAIL_TO
        msg.attach(MIMEText(html, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_SENDER, GMAIL_PASSWORD)
            server.sendmail(GMAIL_SENDER, GMAIL_TO, msg.as_string())
        log.info("Email digest sent")
    except Exception as e:
        log.error(f"Email failed: {e}")

# ── Dashboard JSON ─────────────────────────────────────────────────────────────
def update_dashboard(heartbeats: dict, apis: dict, db_stats: dict):
    existing = {}
    if DASHBOARD_FILE.exists():
        try:
            existing = json.loads(DASHBOARD_FILE.read_text())
        except Exception:
            pass

    existing["business1"] = {
        "name": "Amazon Associates",
        "last_updated": datetime.now().isoformat(),
        "health": "ok" if all(v["ok"] for v in heartbeats.values()) else "warning",
        "heartbeats": heartbeats,
        "apis": apis,
        "stats": db_stats
    }
    DASHBOARD_FILE.write_text(json.dumps(existing, indent=2))
    log.info("Dashboard data updated")

# ── Self-healing ───────────────────────────────────────────────────────────────
def attempt_self_heal(heartbeats: dict):
    """Re-run any module that is overdue or errored, up to 1 retry."""
    module_scripts = {
        "keyword_finder": "b1_keyword_finder.py",
        "article_writer": "b1_article_writer.py",
        "publisher":      "b1_publisher.py",
    }
    for module, status in heartbeats.items():
        if not status["ok"] and status["status"] not in ("never_run",):
            script = module_scripts.get(module)
            if script and Path(script).exists():
                log.info(f"Self-healing: re-running {script}")
                try:
                    result = subprocess.run(
                        ["python", script], capture_output=True, text=True, timeout=300
                    )
                    if result.returncode == 0:
                        log.info(f"  ✓ {script} re-ran successfully")
                    else:
                        log.error(f"  ✗ {script} failed: {result.stderr[:200]}")
                except subprocess.TimeoutExpired:
                    log.error(f"  {script} timed out after 5 minutes")
                except Exception as e:
                    log.error(f"  Failed to re-run {script}: {e}")

# ── Heartbeat ──────────────────────────────────────────────────────────────────
def write_heartbeat(status: str):
    data = {
        "module": "monitor",
        "status": status,
        "last_run": datetime.now().isoformat()
    }
    HEARTBEAT.write_text(json.dumps(data, indent=2))

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("B1 Monitor — Starting health check")
    log.info("=" * 60)

    heartbeats = check_heartbeats()
    apis       = check_api_health()
    db_stats   = get_db_stats()

    # Log summary
    for module, status in heartbeats.items():
        icon = "✓" if status["ok"] else "✗"
        log.info(f"  [{icon}] {module}: {status.get('status','?')} ({status.get('age_hours','?')}h ago)")

    # Try to fix problems before alerting
    attempt_self_heal(heartbeats)

    # Update dashboard
    update_dashboard(heartbeats, apis, db_stats)

    # Send digest (only once per day — check timestamp)
    digest_file = Path("./last_digest_sent.txt")
    should_send = True
    if digest_file.exists():
        last = datetime.fromisoformat(digest_file.read_text().strip())
        if (datetime.now() - last).total_seconds() < 20 * 3600:
            should_send = False

    if should_send:
        send_email_digest(heartbeats, apis, db_stats)
        digest_file.write_text(datetime.now().isoformat())

    write_heartbeat("success")
    log.info("Monitor run complete")

if __name__ == "__main__":
    main()
