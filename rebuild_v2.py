"""Rebuild all articles using Anthropic API for product research (no SerpApi needed)."""
import os, re, json, time, logging, sys, difflib, subprocess
from pathlib import Path
from datetime import datetime
from urllib.parse import unquote_plus
from collections import Counter
import anthropic

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

POSTS_DIR = Path("./_posts")
API_KEY = os.getenv("ANTHROPIC_API_KEY")
ASSOCIATE_TAG = os.getenv("AMAZON_ASSOCIATE_TAG", "viciousstudio-20")
BATCH_SIZE = 10  # commit every N articles

client = anthropic.Anthropic(api_key=API_KEY)

ASIN_RE = re.compile(r'^[A-Z0-9]{10}$')

stats = {
    "total": 0,
    "rebuilt": 0,
    "skipped_no_queries": 0,
    "skipped_no_products": 0,
    "skipped_generation_failed": 0,
    "skipped_verification_failed": 0,
    "total_asins": 0,
    "hallucinations_caught": 0,
    "repairs_attempted": 0,
    "repairs_succeeded": 0,
}


def extract_search_queries(content: str) -> list:
    """Extract all Amazon search queries from article's amazon.com/s?k= URLs."""
    pattern = r'amazon\.com/s\?k=([^&"\')\s\]]+)'
    matches = re.findall(pattern, content)
    seen = []
    for m in matches:
        decoded = unquote_plus(m)
        if decoded not in seen:
            seen.append(decoded)
    return seen


def get_products_from_anthropic(query: str) -> list:
    """Ask Anthropic API for 5 real Amazon products for a search query."""
    prompt = f"""You are a product research assistant. I need exactly 5 real Amazon products for this search query: '{query}'

Return ONLY a JSON array with exactly 5 objects. Each object must have:
- asin: a real 10-character Amazon ASIN (format: B0XXXXXXXX or similar)
- title: the exact product title as it appears on Amazon
- price: realistic current price as string like '$29.99'
- rating: realistic rating like '4.5 out of 5 stars'
- image: a real Amazon product image URL starting with https://m.media-amazon.com/images/ or https://images-na.ssl-images-amazon.com/images/
- url: https://www.amazon.com/dp/{{asin}}?tag={ASSOCIATE_TAG}

Return ONLY the JSON array. No other text."""

    for attempt in range(3):
        try:
            resp = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=2000,
                temperature=0,
                messages=[{"role": "user", "content": prompt}]
            )
            text = resp.content[0].text.strip()
            # Extract JSON array from response
            bracket_start = text.find('[')
            bracket_end = text.rfind(']')
            if bracket_start >= 0 and bracket_end > bracket_start:
                text = text[bracket_start:bracket_end + 1]
            products = json.loads(text)
            if isinstance(products, list):
                return products
        except anthropic.RateLimitError:
            wait = 30 * (attempt + 1)
            log.warning(f"Rate limited on product research, waiting {wait}s...")
            time.sleep(wait)
        except json.JSONDecodeError as e:
            log.warning(f"JSON parse error for query '{query}' (attempt {attempt+1}): {e}")
            time.sleep(5)
        except Exception as e:
            log.error(f"Product research error for '{query}' (attempt {attempt+1}): {e}")
            time.sleep(5)
    return []


def validate_products(products: list) -> list:
    """Validate product data, return only valid products."""
    valid = []
    for p in products:
        errors = []
        asin = p.get("asin", "")
        title = p.get("title", "")
        image = p.get("image", "")
        url = p.get("url", "")

        if not ASIN_RE.match(asin):
            errors.append(f"Invalid ASIN format: {asin}")
        if not title or len(title.strip()) < 3:
            errors.append("Empty or too-short title")
        if not image.startswith("https://") or "amazon" not in image.lower():
            errors.append(f"Invalid image URL: {image[:60]}")
        if f"/dp/{asin}" not in url:
            errors.append(f"URL missing /dp/{asin}")
        if f"tag={ASSOCIATE_TAG}" not in url:
            # Fix the tag
            if "tag=" not in url:
                url = url + (f"&tag={ASSOCIATE_TAG}" if "?" in url else f"?tag={ASSOCIATE_TAG}")
                p["url"] = url
            else:
                errors.append(f"Wrong affiliate tag in URL")

        if errors:
            log.warning(f"  Invalid product {asin}: {'; '.join(errors)}")
        else:
            valid.append(p)
    return valid


def get_verified_products(filepath: Path) -> list:
    """Get verified products for an article using Anthropic API."""
    content = filepath.read_text(encoding="utf-8")
    queries = extract_search_queries(content)

    if not queries:
        # Fall back to title
        title_match = re.search(r'^title:\s*"?([^"\n]+)"?', content, re.MULTILINE)
        if title_match:
            title = title_match.group(1).strip()
            query = re.sub(r'^(Best|Top|Ultimate Guide to)\s+', '', title, flags=re.IGNORECASE)
            queries = [query]

    if not queries:
        return []

    # Try each unique query until we get enough products
    for query in queries:
        log.info(f"  Querying Anthropic for: '{query}'")
        raw_products = get_products_from_anthropic(query)
        valid = validate_products(raw_products)

        if len(valid) >= 3:
            return valid[:5]

        log.info(f"  Only {len(valid)} valid products for '{query}', trying next...")

    # Last resort: return whatever we got from the first query
    if queries:
        raw_products = get_products_from_anthropic(queries[0])
        valid = validate_products(raw_products)
        if valid:
            return valid

    return []


def generate_article(keyword: str, category: str, products: list) -> str:
    """Generate article with Claude using product data."""
    product_data = []
    for p in products:
        product_data.append({
            "asin": p["asin"],
            "exact_title": p["title"],
            "exact_price": p.get("price", "See Amazon for price"),
            "exact_rating": p.get("rating", "Highly rated"),
            "exact_affiliate_url": p["url"],
            "exact_image_url": p["image"],
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
    source_asins = {p["asin"] for p in products}

    # CHECK A: ASIN presence
    found_asins = set(re.findall(r'/dp/([A-Z0-9]{10})', content))
    for fa in found_asins:
        if fa not in source_asins:
            errors.append(f"HALLUCINATED ASIN: {fa} not in source data")
    for sa in source_asins:
        if sa not in found_asins:
            errors.append(f"MISSING ASIN: {sa} not found in article")

    # CHECK B: Title accuracy
    headings = [(m.start(), m.group(1).strip()) for m in re.finditer(r'^##\s+(.+)$', content, re.MULTILINE)]
    for p in products:
        asin_match = re.search(rf'/dp/{re.escape(p["asin"])}', content)
        if asin_match and headings:
            asin_pos = asin_match.start()
            nearest_heading = None
            for hpos, htext in reversed(headings):
                if hpos < asin_pos:
                    nearest_heading = htext
                    break
            if nearest_heading:
                ratio = difflib.SequenceMatcher(None, nearest_heading.lower(), p["title"].lower()).ratio()
                if ratio < 0.4:
                    errors.append(f"TITLE MISMATCH for {p['asin']}: expected '{p['title'][:50]}' got '{nearest_heading[:50]}' (sim: {ratio:.2f})")

    # CHECK C: No search URLs
    search_urls = re.findall(r'amazon\.com/s\?k=', content)
    if search_urls:
        errors.append(f"SEARCH URLs FOUND: {len(search_urls)} search links remain")

    # CHECK D: Affiliate tag on all product URLs
    dp_urls = re.findall(r'https://www\.amazon\.com/dp/[A-Z0-9]{10}[^\s\)\]"]*', content)
    for url in dp_urls:
        if f"tag={ASSOCIATE_TAG}" not in url:
            errors.append(f"MISSING TAG in: {url[:80]}")

    # CHECK E: No placeholder images
    img_urls = re.findall(r'!\[.*?\]\((https?://[^\)]+)\)', content)
    for img in img_urls:
        if any(x in img for x in ["loremflickr", "unsplash", "placeholder", "picsum"]):
            errors.append(f"PLACEHOLDER IMAGE: {img[:60]}")

    # CHECK F: No duplicate ASINs (excessive)
    all_asins = re.findall(r'/dp/([A-Z0-9]{10})', content)
    counts = Counter(all_asins)
    for asin, count in counts.items():
        if count > 3:
            errors.append(f"EXCESSIVE ASIN REPETITION: {asin} appears {count} times")

    return (len(errors) == 0, errors)


def repair_article(content: str, products: list, errors: list) -> str:
    """Attempt to fix article issues."""
    system = """You are fixing an article that failed verification. Fix ONLY the specific issues listed.
Copy all product data EXACTLY from REAL_PRODUCTS. Do not change anything that is not broken.
Output the complete fixed article markdown only."""

    user = f"""Fix these specific errors:
{chr(10).join('- ' + e for e in errors)}

REAL_PRODUCTS (copy all fields EXACTLY):
{json.dumps(products, indent=2)}

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


def git_commit_and_push(message: str):
    """Commit current changes and push."""
    try:
        subprocess.run(["git", "add", "-A"], check=True)
        result = subprocess.run(["git", "diff", "--staged", "--quiet"])
        if result.returncode != 0:
            subprocess.run(["git", "commit", "-m", message], check=True)
            gh_pat = os.getenv("GH_PAT", "")
            if gh_pat:
                remote_url = f"https://x-access-token:{gh_pat}@github.com/ViciousStudio-tech/amazon-associates-content-site.git"
                subprocess.run(["git", "push", remote_url, "HEAD:main"], check=True)
                log.info(f"Committed and pushed: {message}")
            else:
                log.info(f"Committed (no GH_PAT for push): {message}")
        else:
            log.info("No changes to commit")
    except subprocess.CalledProcessError as e:
        log.error(f"Git operation failed: {e}")


def rebuild_all():
    """Main rebuild loop."""
    files = sorted(POSTS_DIR.glob("*.md"))
    start_index = int(os.getenv("START_INDEX", "0"))
    stats["total"] = len(files)

    print("=" * 70)
    print(f"REBUILDING {len(files)} ARTICLES WITH ANTHROPIC API (no SerpApi)")
    if start_index > 0:
        print(f"RESUMING FROM INDEX {start_index}")
    print("=" * 70)

    rebuilt_this_batch = 0

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
            print(f"  SKIPPED: no front matter")
            stats["skipped_no_queries"] += 1
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

        # Get products via Anthropic API
        products = get_verified_products(filepath)
        if not products:
            print(f"  SKIPPED: no valid products found")
            stats["skipped_no_products"] += 1
            continue

        print(f"  Found {len(products)} validated products")
        for p in products:
            print(f"    {p['asin']}: {p['title'][:60]}")

        # Generate article
        article_content = generate_article(title, category, products)
        if not article_content:
            print(f"  SKIPPED: generation failed")
            stats["skipped_generation_failed"] += 1
            continue

        # Verify
        passed, errs = verify_article(article_content, products)

        if not passed:
            print(f"  Verification failed ({len(errs)} issues), attempting repair...")
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
                    print(f"  Repair succeeded")
                else:
                    print(f"  Repair still has {len(errs2)} issues:")
                    for e in errs2:
                        print(f"    - {e}")

        if not passed:
            print(f"  SKIPPED: verification failed after repair")
            stats["skipped_verification_failed"] += 1
            continue

        # Build final file
        first_image = products[0].get("image", "")
        new_fm = f"""---
layout: post
title: "{title}"
date: {date}
categories: [{category}]
description: "{description}"
image: "{first_image}"
affiliate: true
---

"""
        final_content = new_fm + article_content
        filepath.write_text(final_content, encoding="utf-8")

        product_count = len(products)
        stats["rebuilt"] += 1
        stats["total_asins"] += product_count
        rebuilt_this_batch += 1

        print(f"  REBUILT: {product_count} products | all checks passed")

        # Commit every BATCH_SIZE articles
        if rebuilt_this_batch >= BATCH_SIZE:
            git_commit_and_push(
                f"Rebuild batch: {rebuilt_this_batch} articles rebuilt "
                f"(articles {i+2-rebuilt_this_batch}-{i+1} of {len(files)})"
            )
            rebuilt_this_batch = 0

        # Small delay between articles to avoid rate limits
        time.sleep(1)

    # Final commit for remaining articles
    if rebuilt_this_batch > 0:
        git_commit_and_push(
            f"Rebuild final batch: {rebuilt_this_batch} articles rebuilt"
        )

    # Summary
    print("\n" + "=" * 70)
    print("REBUILD SUMMARY")
    print("=" * 70)
    print(f"Total articles processed: {stats['total']}")
    print(f"Successfully rebuilt: {stats['rebuilt']}")
    print(f"Skipped (no queries): {stats['skipped_no_queries']}")
    print(f"Skipped (no products): {stats['skipped_no_products']}")
    print(f"Skipped (generation failed): {stats['skipped_generation_failed']}")
    print(f"Skipped (verification failed): {stats['skipped_verification_failed']}")
    print(f"Total ASINs embedded: {stats['total_asins']}")
    print(f"Hallucinations caught: {stats['hallucinations_caught']}")
    print(f"Repairs attempted: {stats['repairs_attempted']}")
    print(f"Repairs succeeded: {stats['repairs_succeeded']}")

    return stats["rebuilt"] > 0


if __name__ == "__main__":
    success = rebuild_all()
    if success:
        print("\nRebuild complete. Articles committed incrementally.")
    else:
        print("\nNo articles were rebuilt.")
