"""
b1_keyword_finder.py
Finds 20 low-competition, high-buyer-intent Amazon affiliate keywords weekly.
Saves to SQLite DB. Writes heartbeat file so watchdog knows it ran.
"""

import os, json, sqlite3, logging, time
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import anthropic

load_dotenv()

# ── Config ──────────────────────────────────────────────────────────────────
DB_PATH    = os.getenv("DB_PATH", "./data/business1.db")
LOG_DIR    = Path("./logs")
HEARTBEAT  = Path("./heartbeat_keyword_finder.json")
API_KEY    = os.getenv("ANTHROPIC_API_KEY")

LOG_DIR.mkdir(exist_ok=True)
Path("./data").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / f"keyword_finder_{datetime.now():%Y%m%d}.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ── Database ─────────────────────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS keywords (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword TEXT UNIQUE,
            category TEXT,
            buyer_intent_score REAL,
            competition TEXT,
            estimated_searches TEXT,
            commission_rate TEXT,
            status TEXT DEFAULT 'pending',
            created_at TEXT DEFAULT (datetime('now')),
            used_at TEXT
        )
    """)
    conn.commit()
    return conn

# ── Core logic ────────────────────────────────────────────────────────────────
NICHE_CATEGORIES = [
    "Home Decor & Aesthetic", "LED & Ambient Lighting", "Kitchen Gadgets & Organizers",
    "Smart Home Devices", "Wall Art & Prints", "Storage & Organization",
    "Cozy Home Essentials", "Desk & Workspace Setup", "Indoor Plants & Planters",
    "Minimalist Home Accessories"
]

SYSTEM_PROMPT = """You are an expert Amazon affiliate keyword researcher with 10 years of experience.
Your job is to find keywords that:
1. Have HIGH buyer intent (people ready to purchase)
2. Are LOW competition on Google (long-tail, specific)
3. Target products available on Amazon with good commission rates
4. Format: "best [product] for [specific use case/person]"

Return ONLY valid JSON. No markdown fences, no preamble."""

def find_keywords(client, category: str) -> list:
    """Ask Claude to generate keywords for a category. Returns list of dicts."""
    user_prompt = f"""Generate 20 Amazon affiliate article keywords for the '{category}' niche.

Each keyword must:
- Be 4-8 words long (long-tail)
- Have high buyer intent (someone about to spend money)
- Have relatively low Google competition
- Target a specific product type or use case

Return a JSON array of objects with these exact fields:
[
  {{
    "keyword": "best wireless earbuds under 50 dollars",
    "buyer_intent_score": 9.2,
    "competition": "low",
    "estimated_searches": "8,000/mo",
    "commission_rate": "3-4%",
    "why_it_wins": "Specific price point, ready-to-buy intent"
  }}
]

Focus on 2026 trends. Include some seasonal and evergreen keywords."""

    for attempt in range(3):
        try:
            resp = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=2000,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_prompt}]
            )
            raw = resp.content[0].text.strip()
            # Strip any accidental markdown fences
            raw = raw.replace("```json", "").replace("```", "").strip()
            keywords = json.loads(raw)
            log.info(f"  Got {len(keywords)} keywords for {category}")
            return keywords
        except json.JSONDecodeError as e:
            log.warning(f"  JSON parse error (attempt {attempt+1}): {e}")
            time.sleep(2 ** attempt)
        except anthropic.RateLimitError:
            wait = 30 * (attempt + 1)
            log.warning(f"  Rate limit — waiting {wait}s")
            time.sleep(wait)
        except Exception as e:
            log.error(f"  Error (attempt {attempt+1}): {e}")
            time.sleep(5)
    return []

def save_keywords(conn, keywords: list, category: str):
    saved = 0
    for kw in keywords:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO keywords
                  (keyword, category, buyer_intent_score, competition, estimated_searches, commission_rate)
                VALUES (?,?,?,?,?,?)
            """, (
                kw.get("keyword","").lower().strip(),
                category,
                float(kw.get("buyer_intent_score", 7.0)),
                kw.get("competition","medium"),
                kw.get("estimated_searches","unknown"),
                kw.get("commission_rate","3-4%")
            ))
            saved += 1
        except Exception as e:
            log.warning(f"  Could not save keyword: {e}")
    conn.commit()
    return saved

def write_heartbeat(status: str, keywords_found: int):
    data = {
        "module": "keyword_finder",
        "status": status,
        "keywords_found": keywords_found,
        "last_run": datetime.now().isoformat(),
        "next_run": "Next Sunday 8am UTC"
    }
    HEARTBEAT.write_text(json.dumps(data, indent=2))
    log.info(f"Heartbeat written: {status}")

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("B1 Keyword Finder — Starting")
    log.info("=" * 60)

    if not API_KEY:
        log.error("ANTHROPIC_API_KEY not set in .env")
        write_heartbeat("error_no_api_key", 0)
        return

    client = anthropic.Anthropic(api_key=API_KEY)
    conn = init_db()

    total_saved = 0
    # Run 5 categories per execution (~100 keywords to feed 20 articles)
    import random
    selected_categories = random.sample(NICHE_CATEGORIES, 5)

    for category in selected_categories:
        log.info(f"Researching category: {category}")
        keywords = find_keywords(client, category)
        if keywords:
            saved = save_keywords(conn, keywords, category)
            total_saved += saved
            log.info(f"  Saved {saved} keywords for {category}")
        time.sleep(2)  # Be gentle with the API

    # Summary
    cursor = conn.execute("SELECT COUNT(*) FROM keywords WHERE status='pending'")
    pending = cursor.fetchone()[0]
    log.info(f"Run complete. New keywords: {total_saved}. Total pending: {pending}")

    write_heartbeat("success", total_saved)
    conn.close()

if __name__ == "__main__":
    main()
