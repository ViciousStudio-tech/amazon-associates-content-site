"""
b1_publisher.py
Pushes _posts/*.md articles to a GitHub repo for Jekyll / GitHub Pages publishing.
"""

from __future__ import annotations
import os, json, sqlite3, logging, time, base64
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import requests

load_dotenv()

GITHUB_TOKEN  = os.getenv("GITHUB_TOKEN")
GITHUB_REPO   = os.getenv("GITHUB_REPO")       # e.g. "yourusername/content-site"
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "main")
DB_PATH       = os.getenv("DB_PATH", "./data/business1.db")
POSTS_DIR     = Path("./_posts")
LOG_DIR       = Path("./logs")
HEARTBEAT     = Path("./heartbeat_publisher.json")

LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / f"publisher_{datetime.now():%Y%m%d}.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28"
}
BASE_URL = f"https://api.github.com/repos/{GITHUB_REPO}"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    return conn

def get_unpublished_articles(conn) -> list:
    rows = conn.execute("""
        SELECT id, keyword, filename
        FROM articles
        WHERE status = 'draft'
        ORDER BY created_at ASC
    """).fetchall()
    return [{"id": r[0], "keyword": r[1], "filename": r[2]} for r in rows]

def file_exists_on_github(filename: str) -> str | None:
    """Returns current SHA if file exists, else None."""
    url = f"{BASE_URL}/contents/_posts/{filename}"
    for attempt in range(3):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                return resp.json().get("sha")
            elif resp.status_code == 404:
                return None
            elif resp.status_code == 429:
                time.sleep(30 * (attempt + 1))
        except requests.RequestException as e:
            log.warning(f"GitHub check failed (attempt {attempt+1}): {e}")
            time.sleep(5)
    return None

def push_file_to_github(filename: str, content: str) -> bool:
    """Commits a file to GitHub. Creates or updates."""
    url = f"{BASE_URL}/contents/_posts/{filename}"
    encoded = base64.b64encode(content.encode("utf-8")).decode()
    existing_sha = file_exists_on_github(filename)

    payload = {
        "message": f"Add article: {filename}",
        "content": encoded,
        "branch": GITHUB_BRANCH
    }
    if existing_sha:
        payload["sha"] = existing_sha
        payload["message"] = f"Update article: {filename}"

    for attempt in range(3):
        try:
            resp = requests.put(url, headers=HEADERS, json=payload, timeout=30)
            if resp.status_code in (200, 201):
                log.info(f"  ✓ Pushed: {filename}")
                return True
            elif resp.status_code == 429:
                wait = 60 * (attempt + 1)
                log.warning(f"  Rate limit — waiting {wait}s")
                time.sleep(wait)
            else:
                log.error(f"  GitHub error {resp.status_code}: {resp.text[:200]}")
                return False
        except requests.RequestException as e:
            log.error(f"  Push failed (attempt {attempt+1}): {e}")
            time.sleep(10)
    return False

def mark_published(conn, article_id: int):
    conn.execute("""
        UPDATE articles SET status='published', published_at=datetime('now')
        WHERE id=?
    """, (article_id,))
    conn.commit()

def ensure_jekyll_config():
    """Creates a minimal _config.yml and index.md if they don't exist locally."""
    config = Path("./_config.yml")
    if not config.exists():
        config.write_text("""title: "Best Product Reviews"
description: "Expert reviews and buying guides for the best products on Amazon"
baseurl: ""
plugins:
  - jekyll-feed
  - jekyll-seo-tag
""")
    index = Path("./index.md")
    if not index.exists():
        index.write_text("""---
layout: home
---
""")

def push_config_files():
    """Push _config.yml and index.md to GitHub if not there."""
    files_to_push = [
        ("_config.yml", "./_config.yml"),
        ("index.md", "./index.md"),
    ]
    for remote_path, local_path in files_to_push:
        lp = Path(local_path)
        if lp.exists():
            url = f"{BASE_URL}/contents/{remote_path}"
            existing_sha = None
            check = requests.get(url, headers=HEADERS, timeout=10)
            if check.status_code == 200:
                existing_sha = check.json().get("sha")
            elif check.status_code == 404:
                pass  # File doesn't exist yet

            encoded = base64.b64encode(lp.read_bytes()).decode()
            payload = {
                "message": f"Add/update {remote_path}",
                "content": encoded,
                "branch": GITHUB_BRANCH
            }
            if existing_sha:
                payload["sha"] = existing_sha
            resp = requests.put(url, headers=HEADERS, json=payload, timeout=30)
            if resp.status_code in (200, 201):
                log.info(f"  ✓ Config pushed: {remote_path}")

def write_heartbeat(status: str, published: int):
    data = {
        "module": "publisher",
        "status": status,
        "articles_published": published,
        "last_run": datetime.now().isoformat(),
        "repo": GITHUB_REPO
    }
    HEARTBEAT.write_text(json.dumps(data, indent=2))

def main():
    log.info("=" * 60)
    log.info("B1 Publisher — Starting")
    log.info("=" * 60)

    if not GITHUB_TOKEN or not GITHUB_REPO:
        log.error("GITHUB_TOKEN or GITHUB_REPO not set in .env")
        write_heartbeat("error_missing_config", 0)
        return

    conn = init_db()
    ensure_jekyll_config()

    # Push site config files first (idempotent)
    log.info("Ensuring site config files are on GitHub...")
    push_config_files()

    # Get articles that haven't been published yet
    articles = get_unpublished_articles(conn)
    if not articles:
        log.info("No unpublished articles. Run b1_article_writer.py first.")
        write_heartbeat("no_articles", 0)
        conn.close()
        return

    log.info(f"Publishing {len(articles)} articles")
    published = 0

    for article in articles:
        filepath = POSTS_DIR / article["filename"]
        if not filepath.exists():
            log.warning(f"  File not found locally: {filepath}. Skipping.")
            continue

        content = filepath.read_text(encoding="utf-8")
        log.info(f"Publishing: {article['filename']}")

        if push_file_to_github(article["filename"], content):
            mark_published(conn, article["id"])
            published += 1
            time.sleep(2)
        else:
            log.error(f"  Failed to publish: {article['filename']}")

    site_url = f"https://{GITHUB_REPO.split('/')[0]}.github.io/{GITHUB_REPO.split('/')[1]}"
    log.info(f"Done. Published: {published}. Site: {site_url}")
    write_heartbeat("success", published)
    conn.close()

if __name__ == "__main__":
    main()
