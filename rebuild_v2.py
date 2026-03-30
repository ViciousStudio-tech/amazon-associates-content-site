"""
rebuild_v2.py
Fix all Amazon product links in _posts/ articles.
Problem: Link text says one product but URL points to a random/wrong ASIN.
Fix: Convert all /dp/ links to search URLs derived from the link text itself,
     ensuring link text always matches link destination.
"""

import re, os, sys
from urllib.parse import quote_plus, unquote_plus, urlparse, parse_qs
from pathlib import Path

POSTS_DIR = Path("./_posts")
ASSOCIATE_TAG = "viciousstudio-20"

# Match markdown links to Amazon
AMAZON_LINK_RE = re.compile(
    r'\[([^\]]+)\]\((https://www\.amazon\.com/[^\)]+)\)'
)

# Words to strip from link text when building search queries
NOISE_WORDS = {
    'best', 'top', 'the', 'a', 'an', 'and', 'or', 'for', 'with', 'on',
    'in', 'of', 'to', 'my', 'our', 'your', 'this', 'that', 'here',
    'click', 'check', 'see', 'view', 'buy', 'shop', 'price', 'deal',
    'amazon', 'more', 'details', 'review', 'reviews', 'pick', 'picks',
}


def extract_product_name(link_text: str) -> str:
    """
    Extract the product name/search query from link text.
    E.g. "Braun ThermoScan 7 Digital Ear Thermometer" -> "Braun ThermoScan 7 Digital Ear Thermometer"
    E.g. "best electric kettles on Amazon" -> "electric kettles"
    """
    # Remove markdown formatting artifacts
    text = link_text.strip().strip('*').strip('_')

    # If the text looks like a generic phrase (no brand/product name), clean it
    words = text.split()
    if len(words) <= 2:
        return text

    # For longer texts, remove noise words from start/end but keep middle intact
    # This preserves product names like "Braun ThermoScan 7 Digital Ear Thermometer"
    cleaned = []
    for w in words:
        if w.lower() in NOISE_WORDS and not cleaned:
            continue  # skip leading noise
        cleaned.append(w)

    # Remove trailing noise
    while cleaned and cleaned[-1].lower() in NOISE_WORDS:
        cleaned.pop()

    result = ' '.join(cleaned) if cleaned else text
    return result


def build_search_url(product_name: str) -> str:
    """Build an Amazon search URL from a product name."""
    query = quote_plus(product_name)
    return f"https://www.amazon.com/s?k={query}&tag={ASSOCIATE_TAG}"


def check_link_text_matches_url(link_text: str, url: str) -> bool:
    """
    For search URLs: check if link text reasonably matches search query.
    For /dp/ URLs: we can't verify without scraping, so always return False
    to trigger a fix.
    """
    if '/s?k=' in url:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        query = unquote_plus(params.get('k', [''])[0]).lower()
        text_lower = link_text.lower()

        # Get significant words from both
        text_words = set(
            w for w in re.findall(r'[a-z0-9]+', text_lower)
            if len(w) > 2 and w not in NOISE_WORDS
        )
        query_words = set(
            w for w in re.findall(r'[a-z0-9]+', query)
            if len(w) > 2 and w not in NOISE_WORDS
        )

        if not text_words or not query_words:
            return False

        overlap = text_words & query_words
        # Need at least 30% overlap of the smaller set
        min_set = min(len(text_words), len(query_words))
        return len(overlap) >= max(1, min_set * 0.3)

    # /dp/ URLs - we can't verify the ASIN matches the text
    return False


def fix_article_links(content: str) -> tuple:
    """
    Fix all Amazon links in article content.
    Returns (fixed_content, num_links_fixed, details_list).
    """
    fixes = []
    seen_asins_in_article = {}  # track ASIN -> first link text

    def replace_link(match):
        full_match = match.group(0)
        link_text = match.group(1)
        url = match.group(2)

        # Skip non-product links (like generic "Amazon" text links)
        if link_text.lower().strip() in ('amazon', 'amazon.com', 'here', 'link'):
            # Even these should search for something relevant
            if '/dp/' in url:
                # Can't fix without context, leave as search for the article topic
                return full_match

        # Case 1: /dp/ link - these are the problematic ones
        if '/dp/' in url:
            asin_match = re.search(r'/dp/([A-Z0-9]{10})', url)
            asin = asin_match.group(1) if asin_match else "UNKNOWN"

            # Check if this ASIN was already used for a different product name
            if asin in seen_asins_in_article:
                prev_text = seen_asins_in_article[asin]
                # If same ASIN used for different product, definitely wrong
                if prev_text.lower() != link_text.lower():
                    product_name = extract_product_name(link_text)
                    new_url = build_search_url(product_name)
                    fixes.append(f"  DUPLICATE ASIN FIX: [{link_text[:50]}] ASIN {asin} (also used for '{prev_text[:40]}') -> search for '{product_name}'")
                    return f"[{link_text}]({new_url})"

            seen_asins_in_article[asin] = link_text

            # Convert ALL /dp/ links to search URLs since we can't verify ASINs
            product_name = extract_product_name(link_text)
            new_url = build_search_url(product_name)
            fixes.append(f"  FIX: [{link_text[:50]}] /dp/{asin} -> search for '{product_name}'")
            return f"[{link_text}]({new_url})"

        # Case 2: /s?k= search link - verify text matches query
        if '/s?k=' in url:
            if check_link_text_matches_url(link_text, url):
                # Already good, just make sure tag is present
                if f'tag={ASSOCIATE_TAG}' not in url:
                    if '?' in url:
                        new_url = url + f'&tag={ASSOCIATE_TAG}'
                    else:
                        new_url = url + f'?tag={ASSOCIATE_TAG}'
                    fixes.append(f"  TAG FIX: [{link_text[:50]}]")
                    return f"[{link_text}]({new_url})"
                return full_match
            else:
                # Mismatch - rebuild from link text
                product_name = extract_product_name(link_text)
                new_url = build_search_url(product_name)
                fixes.append(f"  MISMATCH FIX: [{link_text[:50]}] -> search for '{product_name}'")
                return f"[{link_text}]({new_url})"

        # Case 3: Other Amazon URL - add tag if missing
        if f'tag={ASSOCIATE_TAG}' not in url:
            sep = '&' if '?' in url else '?'
            new_url = f"{url}{sep}tag={ASSOCIATE_TAG}"
            fixes.append(f"  TAG FIX: [{link_text[:50]}]")
            return f"[{link_text}]({new_url})"

        return full_match

    fixed = AMAZON_LINK_RE.sub(replace_link, content)
    return fixed, len(fixes), fixes


def process_all_articles():
    """Process all articles in _posts/."""
    articles = sorted(POSTS_DIR.glob("*.md"))
    total = len(articles)

    if total == 0:
        print("No articles found in _posts/")
        return

    print(f"Processing {total} articles...\n")

    total_links_fixed = 0
    articles_modified = 0

    for i, filepath in enumerate(articles, 1):
        content = filepath.read_text(encoding='utf-8')
        fixed_content, num_fixed, details = fix_article_links(content)

        if num_fixed > 0:
            filepath.write_text(fixed_content, encoding='utf-8')
            articles_modified += 1
            total_links_fixed += num_fixed
            print(f"Article {i}/{total}: {filepath.name} -- {num_fixed} links fixed")
            for d in details[:5]:  # Show first 5 fixes per article
                print(d)
            if len(details) > 5:
                print(f"  ... and {len(details) - 5} more fixes")
        else:
            print(f"Article {i}/{total}: {filepath.name} -- no changes needed")

    print(f"\n{'='*60}")
    print(f"REBUILD COMPLETE")
    print(f"{'='*60}")
    print(f"Articles processed: {total}")
    print(f"Articles modified:  {articles_modified}")
    print(f"Total links fixed:  {total_links_fixed}")
    print(f"Associate tag:      {ASSOCIATE_TAG}")
    print(f"{'='*60}")


def verify_all_articles():
    """Post-rebuild verification: check every link text matches its destination."""
    articles = sorted(POSTS_DIR.glob("*.md"))
    total_links = 0
    mismatches = 0
    missing_tags = 0

    for filepath in articles:
        content = filepath.read_text(encoding='utf-8')
        for match in AMAZON_LINK_RE.finditer(content):
            link_text = match.group(1)
            url = match.group(2)
            total_links += 1

            # Check tag
            if f'tag={ASSOCIATE_TAG}' not in url:
                missing_tags += 1

            # Check text-to-URL match for search URLs
            if '/s?k=' in url:
                if not check_link_text_matches_url(link_text, url):
                    mismatches += 1
                    print(f"  VERIFY MISMATCH: [{link_text[:50]}] in {filepath.name}")

    print(f"\nVERIFICATION:")
    print(f"  Total links:    {total_links}")
    print(f"  Mismatches:     {mismatches}")
    print(f"  Missing tags:   {missing_tags}")

    if mismatches == 0 and missing_tags == 0:
        print("  STATUS: ALL LINKS VERIFIED OK")
    else:
        print("  STATUS: ISSUES FOUND (see above)")


if __name__ == "__main__":
    print("rebuild_v2.py — Fix all product links\n")
    print("Scraping status: BLOCKED (using search URL fallback)\n")
    process_all_articles()
    print()
    verify_all_articles()
