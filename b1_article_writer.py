"""
b1_article_writer.py
Writes SEO-optimized Amazon affiliate articles using Claude.
Reads pending keywords from DB. Outputs Markdown files to ./_posts/
"""

import os, json, sqlite3, logging, time, re, hashlib
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus, unquote_plus, urlparse, parse_qs
from dotenv import load_dotenv
import anthropic
from amazon_scraper import scrape_amazon_products, verify_products, self_test as scraper_self_test

load_dotenv()

DB_PATH   = os.getenv("DB_PATH", "./data/business1.db")
ASSOCIATE_TAG = os.getenv("AMAZON_ASSOCIATE_TAG", "viciousstudio-20")
POSTS_DIR = Path("./_posts")
LOG_DIR   = Path("./logs")
HEARTBEAT = Path("./heartbeat_article_writer.json")
API_KEY   = os.getenv("ANTHROPIC_API_KEY")
ARTICLES_PER_RUN = int(os.getenv("ARTICLES_PER_RUN", "15"))

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

# Check scraping availability once at module load
SCRAPING_AVAILABLE = False

SYSTEM_PROMPT_WITH_PRODUCTS = f"""You are writing an Amazon affiliate article. You will be given real Amazon products
with their titles, prices, ratings, and direct product URLs. Link directly to these product pages
using the exact URLs provided. NEVER use amazon.com/s?k= search URLs. ALWAYS use direct product
page URLs in format https://www.amazon.com/dp/ASIN?tag={ASSOCIATE_TAG}

Rules:
- Write 1800-2500 words
- Target keyword in: H1 title, first 100 words, 2-3 subheadings, conclusion
- Include product recommendations using ONLY the real products provided — use their actual titles, prices, and ratings
- Affiliate links: use the exact product URLs provided (https://www.amazon.com/dp/ASIN?tag={{ASSOCIATE_TAG}})
- Use h2 and h3 subheadings generously
- Write in first-person expert voice — direct, specific, no filler
- End with a clear "Our Pick" recommendation
- Output raw Markdown ONLY. No preamble. No "Here is the article:" opener."""

SYSTEM_PROMPT_SEARCH_FALLBACK = f"""You are writing an Amazon affiliate article. Since real product data is unavailable,
use Amazon search links so readers can find the products themselves. For EVERY product you mention,
the link text MUST exactly match what the URL searches for.

CRITICAL RULE: When you link to a product, the markdown link text must be the product name, and the
URL must be https://www.amazon.com/s?k={{url_encoded_product_name}}&tag={ASSOCIATE_TAG}
The link text and search query MUST match. Example:
- CORRECT: [Braun ThermoScan 7](https://www.amazon.com/s?k=Braun+ThermoScan+7&tag={ASSOCIATE_TAG})
- WRONG: [Braun ThermoScan 7](https://www.amazon.com/s?k=baby+thermometer&tag={ASSOCIATE_TAG})

Rules:
- Write 1800-2500 words
- Target keyword in: H1 title, first 100 words, 2-3 subheadings, conclusion
- Include 5 specific product recommendations with real brand names and model numbers
- Affiliate links: https://www.amazon.com/s?k={{url_encoded_exact_product_name}}&tag={ASSOCIATE_TAG}
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

def verify_article_links(content: str) -> tuple:
    """
    Verify that every Amazon link's text matches its destination.
    Returns (is_valid, list_of_issues).
    """
    link_re = re.compile(r'\[([^\]]+)\]\((https://www\.amazon\.com/[^\)]+)\)')
    issues = []

    for match in link_re.finditer(content):
        link_text = match.group(1)
        url = match.group(2)

        # Check affiliate tag
        if f'tag={ASSOCIATE_TAG}' not in url:
            issues.append(f"Missing tag: [{link_text[:40]}] -> {url}")

        # For search URLs, verify text matches query
        if '/s?k=' in url:
            parsed = urlparse(url)
            params = parse_qs(parsed.query)
            query = unquote_plus(params.get('k', [''])[0]).lower()
            text_words = set(w.lower() for w in re.findall(r'[a-zA-Z0-9]+', link_text) if len(w) > 2)
            query_words = set(w.lower() for w in re.findall(r'[a-zA-Z0-9]+', query) if len(w) > 2)
            if text_words and query_words:
                overlap = text_words & query_words
                if len(overlap) < 1:
                    issues.append(f"Text/URL mismatch: [{link_text[:40]}] searches for '{query[:40]}'")

    return len(issues) == 0, issues


def fix_article_links_post_generation(content: str) -> str:
    """
    Post-generation fix: ensure every link text matches its URL destination.
    Converts /dp/ links to search URLs derived from link text (since we can't verify ASINs).
    Fixes search URL mismatches.
    """
    link_re = re.compile(r'\[([^\]]+)\]\((https://www\.amazon\.com/[^\)]+)\)')

    def fix_link(match):
        link_text = match.group(1).strip()
        url = match.group(2)

        if '/dp/' in url and not SCRAPING_AVAILABLE:
            # Can't verify ASIN, convert to search URL based on link text
            product_name = link_text.strip('*').strip('_')
            search_url = f"https://www.amazon.com/s?k={quote_plus(product_name)}&tag={ASSOCIATE_TAG}"
            return f"[{link_text}]({search_url})"

        if '/s?k=' in url:
            # Verify the search query matches link text
            parsed = urlparse(url)
            params = parse_qs(parsed.query)
            query = unquote_plus(params.get('k', [''])[0]).lower()
            text_lower = link_text.lower()

            text_words = set(w for w in re.findall(r'[a-z0-9]+', text_lower) if len(w) > 2)
            query_words = set(w for w in re.findall(r'[a-z0-9]+', query) if len(w) > 2)

            if text_words and query_words:
                overlap = text_words & query_words
                if len(overlap) < 1:
                    # Mismatch - rebuild URL from link text
                    product_name = link_text.strip('*').strip('_')
                    search_url = f"https://www.amazon.com/s?k={quote_plus(product_name)}&tag={ASSOCIATE_TAG}"
                    return f"[{link_text}]({search_url})"

            # Ensure tag is present
            if f'tag={ASSOCIATE_TAG}' not in url:
                sep = '&' if '?' in url else '?'
                url = f"{url}{sep}tag={ASSOCIATE_TAG}"
                return f"[{link_text}]({url})"

        return match.group(0)

    return link_re.sub(fix_link, content)


def write_article(client, keyword_data: dict) -> str:
    keyword = keyword_data["keyword"]
    category = keyword_data["category"]

    # First, try to scrape real Amazon products for this keyword
    products = []
    if SCRAPING_AVAILABLE:
        products = scrape_amazon_products(keyword, ASSOCIATE_TAG, 5)
        if products:
            valid, errors = verify_products(products)
            if errors:
                log.warning(f"Product verification errors for '{keyword}': {errors}")
            products = valid

    if products:
        # Store products on keyword_data so build_front_matter can access the first image
        keyword_data["_scraped_products"] = [
            {"asin": p.asin, "title": p.title, "price": p.price,
             "rating": p.rating, "image": p.image, "url": p.url}
            for p in products
        ]

        products_json = json.dumps(keyword_data["_scraped_products"], indent=2)
        system_prompt = SYSTEM_PROMPT_WITH_PRODUCTS

        user_prompt = f"""REAL PRODUCTS (use these exact titles, prices, ratings, and URLs):
{products_json}

Write a complete Amazon affiliate article targeting this keyword:

Keyword: "{keyword}"
Category: {category}
Commission rate for this category: {keyword_data['commission']}

Use the real products above for your recommendations. For each product:
- Use its actual title, price, and rating from the data
- Link directly to its product page URL (the 'url' field)
- List 2-3 pros and 1-2 cons based on the product
- NEVER use amazon.com/s?k= search URLs — ONLY use the direct /dp/ URLs provided

Structure your article with:
1. Hook intro (address the reader's problem directly)
2. Quick answer / recommendation summary
3. Detailed product reviews (H2 for each product)
4. Buying guide section (what to look for)
5. FAQ section (3-5 questions)
6. Conclusion with "Our Top Pick"

Write the complete article now:"""

    else:
        # Scraping blocked — use search URL fallback
        log.warning(f"Scraping unavailable for '{keyword}' — using search URL fallback")
        system_prompt = SYSTEM_PROMPT_SEARCH_FALLBACK

        user_prompt = f"""Write a complete Amazon affiliate article targeting this keyword:

Keyword: "{keyword}"
Category: {category}
Commission rate for this category: {keyword_data['commission']}

IMPORTANT: For every product you recommend, create a markdown link where:
- The link text is the exact product name (brand + model)
- The URL is https://www.amazon.com/s?k={{url_encoded_exact_product_name}}&tag={ASSOCIATE_TAG}
- The link text and search query MUST be the same product name

Structure your article with:
1. Hook intro (address the reader's problem directly)
2. Quick answer / recommendation summary
3. Detailed product reviews (H2 for each product with 5 specific products)
4. Buying guide section (what to look for)
5. FAQ section (3-5 questions)
6. Conclusion with "Our Top Pick"

Write the complete article now:"""

    for attempt in range(3):
        try:
            resp = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=4000,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}]
            )
            article_text = resp.content[0].text.strip()

            # Post-generation verification and fix
            article_text = fix_article_links_post_generation(article_text)

            is_valid, issues = verify_article_links(article_text)
            if not is_valid:
                log.warning(f"Link verification issues for '{keyword}': {issues}")
                # The fix_article_links_post_generation should have handled these,
                # but log for visibility

            return article_text
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

def fetch_article_image(keyword_data: dict, title: str) -> str:
    """Get image URL from scraped products, falling back to loremflickr."""
    # Use first scraped product image if available
    products = keyword_data.get("_scraped_products", [])
    if products:
        # Handle both dict and dataclass forms
        first = products[0]
        img = first.get("image") if isinstance(first, dict) else getattr(first, "image", None)
        if img and img.startswith("https://"):
            return img

    # Fallback to loremflickr
    stop = {'a','an','the','and','but','or','for','nor','on','at','to','by','in','of','up',
            'with','your','our','is','are','best','top','most','very'}
    words = [w for w in re.sub(r'[^a-z0-9\s]', '', title.lower()).split()
             if w not in stop and len(w) > 2][:3]
    search_term = ",".join(words) if words else "product"
    lock = int(hashlib.md5(title.encode()).hexdigest()[:8], 16) % 100000
    return f"https://loremflickr.com/800/450/{search_term}?lock={lock}"

def build_front_matter(keyword_data: dict, title: str) -> str:
    today = datetime.now().strftime("%Y-%m-%d")
    description = f"Looking for the {keyword_data['keyword']}? Our expert guide covers the top picks with detailed reviews, pros & cons, and a buying guide."
    image_url = fetch_article_image(keyword_data, title)
    return f"""---
layout: post
title: "{title}"
date: {today}
categories: [{keyword_data['category'].lower().replace(' & ', '-').replace(' ', '-')}]
description: "{description}"
image: "{image_url}"
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
    global SCRAPING_AVAILABLE

    log.info("=" * 60)
    log.info("B1 Article Writer — Starting")
    log.info("=" * 60)

    if not API_KEY:
        log.error("ANTHROPIC_API_KEY not set")
        write_heartbeat("error_no_api_key", 0)
        return

    # Check if Amazon scraping is available
    log.info("Testing Amazon scraper availability...")
    SCRAPING_AVAILABLE = scraper_self_test()
    if SCRAPING_AVAILABLE:
        log.info("Scraper: AVAILABLE — will use real product data")
    else:
        log.warning("Scraper: BLOCKED — will use search URL fallback (link text = search query)")

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
