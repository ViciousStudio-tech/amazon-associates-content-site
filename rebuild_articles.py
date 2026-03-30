"""
rebuild_articles.py
Rebuilds all existing articles in _posts/ by replacing fake Amazon search URLs
with real product data scraped from Amazon.

Uses the Anthropic API when available to intelligently rewrite product sections.
Falls back to direct URL replacement when API key is not set.
"""

import os
import sys
import re
import json
import time
import logging
from pathlib import Path
from urllib.parse import unquote_plus

from dotenv import load_dotenv

load_dotenv()

# Add repo root to path for local imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from amazon_scraper import scrape_amazon_products

POSTS_DIR = Path(os.path.dirname(os.path.abspath(__file__))) / "_posts"
ASSOCIATE_TAG = "viciousstudio-20"
API_KEY = os.getenv("ANTHROPIC_API_KEY")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)

# Regex to find Amazon search URLs
SEARCH_URL_RE = re.compile(r'amazon\.com/s\?k=([^&")\]\s]+)')


def extract_search_queries(content: str) -> list:
    """Extract all Amazon search query keywords from article content."""
    matches = SEARCH_URL_RE.findall(content)
    seen = []
    for m in matches:
        decoded = unquote_plus(m)
        if decoded not in seen:
            seen.append(decoded)
    return seen


def replace_search_urls_with_products(content: str, products: list) -> str:
    """
    Replace Amazon search URLs in the article with direct product page URLs.

    Strategy: find each markdown link with an Amazon search URL and replace
    the URL with the best matching real product URL. Cycles through products.
    """
    # Find all markdown links with Amazon search URLs
    link_pattern = re.compile(
        r'\[([^\]]+)\]\(https://www\.amazon\.com/s\?k=[^)]+\)'
    )

    matches = list(link_pattern.finditer(content))
    if not matches or not products:
        return content

    # Replace each link, cycling through products
    result = content
    # Process in reverse order to maintain string positions
    for i, match in enumerate(reversed(matches)):
        product_idx = (len(matches) - 1 - i) % len(products)
        product = products[product_idx]

        old_link = match.group(0)
        link_text = match.group(1)
        new_link = f'[{link_text}]({product["url"]})'

        result = result[:match.start()] + new_link + result[match.end():]

    return result


def update_front_matter_image(content: str, image_url: str) -> str:
    """Update the image field in YAML front matter."""
    # Handle both quoted and unquoted image fields, and multiple image lines
    # Replace the last image: line in front matter (or the only one)
    front_match = re.match(r'^(---\n)(.*?)(---\n)', content, re.DOTALL)
    if not front_match:
        return content

    front = front_match.group(2)

    # Remove all image: lines
    lines = front.split('\n')
    new_lines = [l for l in lines if not l.strip().startswith('image:')]

    # Add the new image line before the closing ---
    new_lines.append(f'image: "{image_url}"')

    new_front = '\n'.join(new_lines)
    return f"---\n{new_front}\n---\n" + content[front_match.end():]


def rewrite_with_anthropic(content: str, products: list) -> str:
    """Use Anthropic API to intelligently rewrite product sections."""
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=API_KEY)

        system_prompt = (
            "You are rewriting product recommendation sections of an Amazon affiliate article. "
            "You will receive the current article markdown and a list of real Amazon products. "
            "Replace every fake product recommendation with the real products provided. "
            "Keep the article structure, intro, buying guide, FAQ, and conclusion intact. "
            "Only replace the product recommendation sections. "
            "For each real product use its actual title, price, rating, and link directly to "
            "its product page URL. Output the complete rewritten article markdown only."
        )

        user_message = (
            f"CURRENT ARTICLE:\n{content}\n\n"
            f"REAL PRODUCTS:\n{json.dumps(products, indent=2)}\n\n"
            "Rewrite the product sections using these real products."
        )

        resp = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4000,
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}],
        )
        return resp.content[0].text.strip()
    except Exception as e:
        log.warning(f"Anthropic API rewrite failed: {e}")
        return None


def rebuild_article(filepath: Path, article_num: int, total: int) -> bool:
    """
    Rebuild a single article with real Amazon products.
    Returns True if successfully rebuilt, False if skipped.
    """
    content = filepath.read_text(encoding="utf-8")
    queries = extract_search_queries(content)

    if not queries:
        log.info(f"  No Amazon search URLs found, skipping")
        return False

    # Try each query until we get results
    products = []
    for query in queries:
        products = scrape_amazon_products(query, ASSOCIATE_TAG, 5)
        if products:
            break
        log.info(f"  Query '{query}' returned 0 results, trying next...")
        time.sleep(1)

    if not products:
        log.warning(f"  All queries blocked/empty for {filepath.name}, skipping")
        return False

    num_products = len(products)

    # Rewrite article
    if API_KEY and API_KEY != "sk-ant-YOUR_KEY_HERE":
        rewritten = rewrite_with_anthropic(content, products)
        if rewritten:
            # Ensure front matter is preserved if API stripped it
            if not rewritten.startswith("---"):
                fm_match = re.match(r'^(---\n.*?---\n)', content, re.DOTALL)
                if fm_match:
                    rewritten = fm_match.group(1) + rewritten
            new_content = rewritten
        else:
            # Fallback to regex replacement
            new_content = replace_search_urls_with_products(content, products)
    else:
        # No API key — use direct URL replacement
        new_content = replace_search_urls_with_products(content, products)

    # Update front matter image with first product image
    if products[0].get("image"):
        new_content = update_front_matter_image(new_content, products[0]["image"])

    filepath.write_text(new_content, encoding="utf-8")
    log.info(f"Article {article_num}/{total}: {filepath.name} — {num_products} real products found")
    return True


def main():
    log.info("=" * 60)
    log.info("Rebuild Articles — Replacing fake search URLs with real products")
    log.info("=" * 60)

    if API_KEY and API_KEY != "sk-ant-YOUR_KEY_HERE":
        log.info("Anthropic API key found — will use AI rewriting")
    else:
        log.info("No Anthropic API key — using direct URL replacement")

    md_files = sorted(POSTS_DIR.glob("*.md"))
    total = len(md_files)
    log.info(f"Found {total} articles to process")

    rebuilt = 0
    skipped = 0

    for i, filepath in enumerate(md_files, 1):
        log.info(f"\n[{i}/{total}] Processing: {filepath.name}")
        success = rebuild_article(filepath, i, total)
        if success:
            rebuilt += 1
        else:
            skipped += 1

        # Sleep between articles to avoid rate limiting
        if i < total:
            time.sleep(2)

    log.info("\n" + "=" * 60)
    log.info(f"REBUILD COMPLETE")
    log.info(f"  Articles rebuilt with real products: {rebuilt}")
    log.info(f"  Articles skipped (no URLs or blocked): {skipped}")
    log.info("=" * 60)

    return rebuilt, skipped


if __name__ == "__main__":
    rebuilt, skipped = main()
