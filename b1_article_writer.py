"""
b1_article_writer.py
Writes SEO-optimized Amazon affiliate articles using Claude.
Reads pending keywords from DB. Outputs Markdown files to ./_posts/
"""

import os, json, sqlite3, logging, time, re
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import anthropic

load_dotenv()

DB_PATH   = os.getenv("DB_PATH", "./data/business1.db")
ASSOCIATE_TAG = os.getenv("AMAZON_ASSOCIATE_TAG", "your-tag-20")
POSTS_DIR = Path("./_posts")
LOG_DIR   = Path("./logs")
HEARTBEAT = Path("./heartbeat_article_writer.json")
API_KEY   = os.getenv("ANTHROPIC_API_KEY")
ARTICLES_PER_RUN = int(os.getenv("ARTICLES_PER_RUN", "3"))

POSTS_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / f"article_writer_{datetime.now():%Y%m%d}.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

SYSTEM_PROMPT = f"""You are an expert Amazon affiliate content writer. Your articles rank on Google 
and convert readers into buyers. You write with genuine authority and specific, helpful detail.

Rules:
- Write 1800-2500 words
- Target keyword in: H1 title, first 100 words, 2-3 subheadings, conclusion  
- Include 5-7 specific product recommendations with pros, cons, price range
- Affiliate links format: [Product Name](https://www.amazon.com/s?k=PRODUCT+SEARCH+QUERY&tag={ASSOCIATE_TAG})
- Use h2 and h3 subheadings generously
- Write in first-person expert voice — direct, specific, no filler
- End with a clear "Our Pick" recommendation
- Output raw Markdown ONLY. No preamble. No "Here is the article:" opener."""

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword_id INTEGER,
            keyword TEXT,
            title TEXT,
            slug TEXT UNIQUE,
            filename TEXT,
            word_count INTEGER,
            status TEXT DEFAULT 'draft',
            created_at TEXT DEFAULT (datetime('now')),
            published_at TEXT,
            FOREIGN KEY(keyword_id) REFERENCES keywords(id)
        )
    """)
    conn.commit()
    return conn

def get_pending_keywords(conn, limit: int) -> list:
    rows = conn.execute("""
        SELECT id, keyword, category, buyer_intent_score, commission_rate
        FROM keywords
        WHERE status = 'pending'
        ORDER BY buyer_intent_score DESC
        LIMIT ?
    """, (limit,)).fetchall()
    return [{"id":r[0],"keyword":r[1],"category":r[2],"score":r[3],"commission":r[4]} for r in rows]

def keyword_to_slug(keyword: str) -> str:
    slug = re.sub(r'[^a-z0-9\s-]', '', keyword.lower())
    slug = re.sub(r'\s+', '-', slug.strip())
    return slug[:80]

def keyword_to_title(keyword: str) -> str:
    """Capitalize like a proper article title."""
    words = keyword.split()
    small = {'a','an','the','and','but','or','for','nor','on','at','to','by','in','of','up'}
    return ' '.join(
        w.capitalize() if i == 0 or w.lower() not in small else w.lower()
        for i, w in enumerate(words)
    )

def write_article(client, keyword_data: dict) -> str:
    keyword = keyword_data["keyword"]
    category = keyword_data["category"]

    user_prompt = f"""Write a complete Amazon affiliate article targeting this keyword:

Keyword: "{keyword}"
Category: {category}
Commission rate for this category: {keyword_data['commission']}

Include 5-7 real Amazon product recommendations. For each product:
- Give it a descriptive name (not a made-up brand)
- Include a realistic price range (e.g., "$35-$55")
- List 2-3 pros and 1-2 cons
- Create an Amazon search affiliate link in this format:
  [Product Name](https://www.amazon.com/s?k=SEARCH+TERMS+HERE&tag={ASSOCIATE_TAG})

Structure your article with:
1. Hook intro (address the reader's problem directly)
2. Quick answer / recommendation summary  
3. Detailed product reviews (H2 for each product)
4. Buying guide section (what to look for)
5. FAQ section (3-5 questions)
6. Conclusion with "Our Top Pick"

Write the complete article now:"""

    for attempt in range(3):
        try:
            resp = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=4000,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_prompt}]
            )
            return resp.content[0].text.strip()
        except anthropic.RateLimitError:
            wait = 60 * (attempt + 1)
            log.warning(f"Rate limit — waiting {wait}s")
            time.sleep(wait)
        except Exception as e:
            log.error(f"Article generation error (attempt {attempt+1}): {e}")
            time.sleep(10)
    return None

def save_article(conn, keyword_data: dict, content: str, filename: str):
    word_count = len(content.split())
    slug = keyword_to_slug(keyword_data["keyword"])
    try:
        conn.execute("""
            INSERT OR IGNORE INTO articles (keyword_id, keyword, title, slug, filename, word_count)
            VALUES (?,?,?,?,?,?)
        """, (keyword_data["id"], keyword_data["keyword"],
              keyword_to_title(keyword_data["keyword"]),
              slug, filename, word_count))
        conn.execute("UPDATE keywords SET status='used', used_at=datetime('now') WHERE id=?",
                     (keyword_data["id"],))
        conn.commit()
        log.info(f"  Saved to DB — {word_count} words")
    except Exception as e:
        log.error(f"DB save error: {e}")

def build_front_matter(keyword_data: dict, title: str) -> str:
    today = datetime.now().strftime("%Y-%m-%d")
    description = f"Looking for the {keyword_data['keyword']}? Our expert guide covers the top picks with detailed reviews, pros & cons, and a buying guide."
    return f"""---
layout: post
title: "{title}"
date: {today}
categories: [{keyword_data['category'].lower().replace(' & ', '-').replace(' ', '-')}]
description: "{description}"
affiliate: true
---

"""

def write_heartbeat(status: str, articles_written: int):
    data = {
        "module": "article_writer",
        "status": status,
        "articles_written": articles_written,
        "last_run": datetime.now().isoformat()
    }
    HEARTBEAT.write_text(json.dumps(data, indent=2))

def main():
    log.info("=" * 60)
    log.info("B1 Article Writer — Starting")
    log.info("=" * 60)

    if not API_KEY:
        log.error("ANTHROPIC_API_KEY not set")
        write_heartbeat("error_no_api_key", 0)
        return

    client = anthropic.Anthropic(api_key=API_KEY)
    conn = init_db()

    keywords = get_pending_keywords(conn, ARTICLES_PER_RUN)
    if not keywords:
        log.info("No pending keywords found. Run b1_keyword_finder.py first.")
        write_heartbeat("no_keywords", 0)
        conn.close()
        return

    log.info(f"Writing {len(keywords)} articles")
    written = 0

    for kw_data in keywords:
        keyword = kw_data["keyword"]
        log.info(f"Writing article for: '{keyword}'")

        content = write_article(client, kw_data)
        if not content:
            log.error(f"  Failed to generate article for: {keyword}")
            continue

        # Build filename (Jekyll format)
        today = datetime.now().strftime("%Y-%m-%d")
        slug = keyword_to_slug(keyword)
        filename = f"{today}-{slug}.md"
        filepath = POSTS_DIR / filename

        # Add Jekyll front matter
        title = keyword_to_title(keyword)
        full_content = build_front_matter(kw_data, title) + content

        filepath.write_text(full_content, encoding="utf-8")
        log.info(f"  Written to: {filepath}")

        save_article(conn, kw_data, content, filename)
        written += 1

        time.sleep(3)  # Brief pause between articles

    log.info(f"Done. Articles written: {written}")
    write_heartbeat("success", written)
    conn.close()

if __name__ == "__main__":
    main()
