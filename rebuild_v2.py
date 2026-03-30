"""Rebuild all articles with verified real Amazon products from SerpApi."""
import os, re, json, time, logging, sys, difflib
from pathlib import Path
from datetime import datetime
from urllib.parse import unquote_plus, quote_plus
from collections import Counter
from dotenv import load_dotenv
import anthropic

load_dotenv()

# Import our scraper
from amazon_scraper import scrape_amazon_products, verify_products, Product, ASSOCIATE_TAG, asdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

POSTS_DIR = Path("./_posts")
API_KEY = os.getenv("ANTHROPIC_API_KEY")
client = anthropic.Anthropic(api_key=API_KEY)

# Stats
stats = {
    "total": 0,
    "rebuilt": 0,
    "skipped_no_products": 0,
    "skipped_verification_failed": 0,
    "total_asins": 0,
    "hallucinations_caught": 0,
    "repairs_attempted": 0,
    "repairs_succeeded": 0,
}


def extract_search_queries(content: str) -> list:
    """Extract all Amazon search queries from article."""
    pattern = r'amazon\.com/s\?k=([^&"\')\s]+)'
    matches = re.findall(pattern, content)
    return [unquote_plus(m) for m in matches]


def get_verified_products(filepath: Path) -> list:
    """Get verified products for an article."""
    content = filepath.read_text(encoding="utf-8")
    queries = extract_search_queries(content)

    if not queries:
        # Fall back to title
        title_match = re.search(r'^title:\s*"?([^"\n]+)"?', content, re.MULTILINE)
        if title_match:
            title = title_match.group(1).strip()
            # Remove "Best" prefix and clean up
            query = re.sub(r'^(Best|Top|Ultimate Guide to)\s+', '', title, flags=re.IGNORECASE)
            queries = [query]

    seen_queries = set()
    for query in queries:
        if query in seen_queries:
            continue
        seen_queries.add(query)

        products = scrape_amazon_products(query, ASSOCIATE_TAG, 5)
        valid, errors = verify_products(products)

        if len(valid) >= 3:
            return valid

        log.info(f"  Only {len(valid)} products for '{query}', trying next query...")

    # Last resort — try with fewer products
    for query in queries:
        products = scrape_amazon_products(query, ASSOCIATE_TAG, 5)
        valid, _ = verify_products(products)
        if valid:
            return valid

    return []


def generate_article(keyword: str, category: str, products: list) -> str:
    """Generate article with Claude using real product data."""
    product_data = []
    for p in products:
        d = asdict(p) if isinstance(p, Product) else p
        product_data.append({
            "asin": d["asin"],
            "exact_title": d["title"],
            "exact_price": d["price"] or "See Amazon for price",
            "exact_rating": d["rating"] or "Highly rated",
            "exact_affiliate_url": d["url"],
            "exact_image_url": d["image"],
        })

    system = f"""You are writing an Amazon affiliate buying guide. You will be given REAL products with verified data.

ABSOLUTE RULES — violating any makes the article INVALID:
1. Use ONLY the products in REAL_PRODUCTS below. Do NOT invent ANY product.
2. Copy each product title EXACTLY as given — zero modifications, zero paraphrasing.
3. Use ONLY the URLs provided. Do NOT construct, modify, or shorten any URL.
4. Write exactly {len(products)} product recommendations — one per product provided. No more. No less.
5. Each product recommendation MUST include:
   - The EXACT title as an H2 heading
   - The EXACT image URL in a markdown image tag
   - The EXACT price
   - The EXACT rating
   - The EXACT affiliate URL as a markdown link with the EXACT title as link text
6. Do NOT mention any brand, product, or ASIN not in REAL_PRODUCTS.
7. Output raw Markdown ONLY. No preamble. No "Here is..." opener."""

    user = f"""Write a complete Amazon affiliate buying guide for: "{keyword}"
Category: {category}

REAL_PRODUCTS (use ONLY these — copy ALL fields EXACTLY):
{json.dumps(product_data, indent=2)}

Article structure:
1. Introduction (150-200 words, address the reader's need)
2. Quick Picks summary (product | price | rating — use exact data)
3. Detailed review for EACH product (in order):
   ## [EXACT title from REAL_PRODUCTS]
   ![product image](EXACT image_url)
   **Price:** EXACT price | **Rating:** EXACT rating
   [200-word review discussing this product type's features and value]
   **[Buy on Amazon](EXACT affiliate_url)**
4. Buying Guide section (what to look for, 300 words)
5. FAQ (3-5 questions with answers)
6. Conclusion with top pick recommendation

Write the complete article now:"""

    for attempt in range(3):
        try:
            resp = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=5000,
                temperature=0,
                system=system,
                messages=[{"role": "user", "content": user}]
            )
            return resp.content[0].text.strip()
        except anthropic.RateLimitError:
            wait = 30 * (attempt + 1)
            log.warning(f"Rate limited, waiting {wait}s...")
            time.sleep(wait)
        except Exception as e:
            log.error(f"Generation error (attempt {attempt+1}): {e}")
            time.sleep(5)
    return None


def verify_article(content: str, products: list) -> tuple:
    """Run all 6 verification checks. Returns (passed: bool, errors: list)."""
    errors = []
    source_asins = set()
    for p in products:
        asin = p.asin if isinstance(p, Product) else p["asin"]
        source_asins.add(asin)

    # CHECK A: ASIN presence
    found_asins = set(re.findall(r'/dp/([A-Z0-9]{10})', content))
    for fa in found_asins:
        if fa not in source_asins:
            errors.append(f"HALLUCINATED ASIN: {fa} not in source data")
    for sa in source_asins:
        if sa not in found_asins:
            errors.append(f"MISSING ASIN: {sa} not found in article")

    # CHECK B: Title accuracy (using difflib)
    # Find all H2 headings with their positions
    headings = [(m.start(), m.group(1).strip()) for m in re.finditer(r'^##\s+(.+)$', content, re.MULTILINE)]
    for p in products:
        product = p if isinstance(p, Product) else Product(**p)
        # Find position of this ASIN in article
        asin_match = re.search(rf'/dp/{re.escape(product.asin)}', content)
        if asin_match and headings:
            asin_pos = asin_match.start()
            # Find the nearest H2 heading BEFORE this ASIN
            nearest_heading = None
            for hpos, htext in reversed(headings):
                if hpos < asin_pos:
                    nearest_heading = htext
                    break
            if nearest_heading:
                ratio = difflib.SequenceMatcher(None, nearest_heading.lower(), product.title.lower()).ratio()
                if ratio < 0.4:
                    errors.append(f"TITLE MISMATCH for {product.asin}: expected '{product.title[:50]}' got '{nearest_heading[:50]}' (sim: {ratio:.2f})")

    # CHECK C: No search URLs
    search_urls = re.findall(r'amazon\.com/s\?k=', content)
    if search_urls:
        errors.append(f"SEARCH URLs FOUND: {len(search_urls)} search links remain")

    # CHECK D: Affiliate tag on all product URLs
    dp_urls = re.findall(r'https://www\.amazon\.com/dp/[A-Z0-9]{10}[^\s\)\]"]*', content)
    for url in dp_urls:
        if "tag=viciousstudio-20" not in url and "tag=" + ASSOCIATE_TAG not in url:
            errors.append(f"MISSING TAG in: {url[:80]}")

    # CHECK E: No placeholder images
    img_urls = re.findall(r'!\[.*?\]\((https?://[^\)]+)\)', content)
    for img in img_urls:
        if any(x in img for x in ["loremflickr", "unsplash", "placeholder", "picsum"]):
            errors.append(f"PLACEHOLDER IMAGE: {img[:60]}")

    # CHECK F: No duplicate ASINs
    all_asins = re.findall(r'/dp/([A-Z0-9]{10})', content)
    counts = Counter(all_asins)
    for asin, count in counts.items():
        if count > 3:  # Allow some repetition for links
            errors.append(f"EXCESSIVE ASIN REPETITION: {asin} appears {count} times")

    return (len(errors) == 0, errors)


def repair_article(content: str, products: list, errors: list) -> str:
    """Attempt to fix article issues."""
    product_data = []
    for p in products:
        d = asdict(p) if isinstance(p, Product) else p
        product_data.append(d)

    system = """You are fixing an article that failed verification. Fix ONLY the specific issues listed.
Copy all product data EXACTLY from REAL_PRODUCTS. Do not change anything that is not broken.
Output the complete fixed article markdown only."""

    user = f"""Fix these specific errors:
{chr(10).join('- ' + e for e in errors)}

REAL_PRODUCTS (copy all fields EXACTLY):
{json.dumps(product_data, indent=2, default=str)}

CURRENT ARTICLE:
{content}

Return the complete fixed article:"""

    try:
        resp = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=5000,
            temperature=0,
            system=system,
            messages=[{"role": "user", "content": user}]
        )
        return resp.content[0].text.strip()
    except Exception as e:
        log.error(f"Repair failed: {e}")
        return None


def rebuild_all():
    """Main rebuild loop."""
    files = sorted(POSTS_DIR.glob("*.md"))
    start_index = int(os.getenv("START_INDEX", "0"))
    stats["total"] = len(files)

    print("=" * 70)
    print(f"REBUILDING {len(files)} ARTICLES WITH REAL AMAZON PRODUCTS (SerpApi)")
    if start_index > 0:
        print(f"RESUMING FROM INDEX {start_index}")
    print("=" * 70)

    rebuilt_files = []

    for i, filepath in enumerate(files):
        if i < start_index:
            continue
        filename = filepath.name
        print(f"\n{'─' * 60}")
        print(f"Article {i+1}/{len(files)}: {filename}")

        # Read existing front matter
        content = filepath.read_text(encoding="utf-8")
        fm_match = re.match(r'^---\n(.*?)\n---\n', content, re.DOTALL)
        if not fm_match:
            print(f"  ❌ SKIPPED: no front matter")
            stats["skipped_no_products"] += 1
            continue

        fm_text = fm_match.group(1)
        title_match = re.search(r'^title:\s*"?([^"\n]+)"?', fm_text, re.MULTILINE)
        date_match = re.search(r'^date:\s*(\S+)', fm_text, re.MULTILINE)
        cat_match = re.search(r'^categories:\s*\[([^\]]+)\]', fm_text, re.MULTILINE)
        desc_match = re.search(r'^description:\s*"?([^"\n]+)"?', fm_text, re.MULTILINE)

        title = title_match.group(1).strip() if title_match else "Product Guide"
        date = date_match.group(1).strip() if date_match else datetime.now().strftime("%Y-%m-%d")
        category = cat_match.group(1).strip() if cat_match else "general"
        description = desc_match.group(1).strip() if desc_match else ""

        # Get verified products
        products = get_verified_products(filepath)
        if not products:
            print(f"  ❌ SKIPPED: no verified products found")
            stats["skipped_no_products"] += 1
            continue

        print(f"  Found {len(products)} verified products")
        for p in products:
            product = p if isinstance(p, Product) else Product(**p)
            print(f"    {product.asin}: {product.title[:50]}...")

        # Generate article
        article_content = generate_article(title, category, products)
        if not article_content:
            print(f"  ❌ SKIPPED: generation failed")
            stats["skipped_verification_failed"] += 1
            continue

        # Verify
        passed, errs = verify_article(article_content, products)

        if not passed:
            print(f"  ⚠️ Verification failed ({len(errs)} issues), attempting repair...")
            for e in errs:
                print(f"    - {e}")
            stats["repairs_attempted"] += 1
            stats["hallucinations_caught"] += len(errs)

            repaired = repair_article(article_content, products, errs)
            if repaired:
                passed2, errs2 = verify_article(repaired, products)
                if passed2:
                    article_content = repaired
                    passed = True
                    stats["repairs_succeeded"] += 1
                    print(f"  ✅ Repair succeeded")
                else:
                    print(f"  ❌ Repair still has {len(errs2)} issues:")
                    for e in errs2:
                        print(f"    - {e}")

        if not passed:
            print(f"  ❌ SKIPPED: verification failed after repair")
            stats["skipped_verification_failed"] += 1
            continue

        # Build final file
        first_product = products[0] if isinstance(products[0], Product) else Product(**products[0])
        new_fm = f"""---
layout: post
title: "{title}"
date: {date}
categories: [{category}]
description: "{description}"
image: "{first_product.image}"
affiliate: true
---

"""
        final_content = new_fm + article_content
        filepath.write_text(final_content, encoding="utf-8")

        product_count = len(products)
        stats["rebuilt"] += 1
        stats["total_asins"] += product_count
        rebuilt_files.append((filepath, products))

        print(f"  ✅ REBUILT: {product_count} products | all checks passed")
        time.sleep(0.5)

    # FINAL AUDIT
    print("\n" + "=" * 70)
    print("FINAL AUDIT — Re-verifying all rebuilt articles")
    print("=" * 70)

    audit_passed = 0
    audit_failed = 0

    for filepath, products in rebuilt_files:
        content = filepath.read_text(encoding="utf-8")
        # Strip front matter for verification
        fm_end = content.find("---", 4)
        if fm_end > 0:
            article_body = content[fm_end+4:]
        else:
            article_body = content

        passed, errs = verify_article(article_body, products)
        if passed:
            audit_passed += 1
        else:
            audit_failed += 1
            print(f"  AUDIT FAIL: {filepath.name}")
            for e in errs:
                print(f"    - {e}")

    print(f"\nFINAL AUDIT: {audit_passed}/{len(rebuilt_files)} passed")

    # Summary
    print("\n" + "=" * 70)
    print("REBUILD SUMMARY")
    print("=" * 70)
    print(f"Total articles processed: {stats['total']}")
    print(f"Successfully rebuilt: {stats['rebuilt']}")
    print(f"Skipped (no products): {stats['skipped_no_products']}")
    print(f"Skipped (verification failed): {stats['skipped_verification_failed']}")
    print(f"Total real ASINs embedded: {stats['total_asins']}")
    print(f"Hallucinations caught: {stats['hallucinations_caught']}")
    print(f"Repairs attempted: {stats['repairs_attempted']}")
    print(f"Repairs succeeded: {stats['repairs_succeeded']}")

    if audit_failed > 0:
        print(f"\n🚫 COMMIT BLOCKED: {audit_failed} articles failed final audit")
        return False

    return True


if __name__ == "__main__":
    success = rebuild_all()
    if success and stats["rebuilt"] > 0:
        print("\nAll articles passed final audit. Ready to commit.")
    elif stats["rebuilt"] == 0:
        print("\nNo articles were rebuilt.")
    else:
        print("\nSome articles failed audit. NOT committing.")
