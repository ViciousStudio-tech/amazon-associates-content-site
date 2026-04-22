# Lessons

## 2026-04-22 — Content pipeline publishing broken Amazon links
MISTAKE: DwellPicks content pipeline was publishing new posts with broken /s?k= search URLs instead of /dp/ASIN links. All 146 posts have zero real ASINs — every single Amazon link is a search URL that earns zero commission. Prior memory estimated ~102 broken; actual count after pull is 146/146 (100%).
ROOT CAUSE: The article generation pipeline never validated that Amazon links contain real ASINs before committing. Claude AI generated search-style URLs and the pipeline accepted them.
RULE: Before publish, reject any post whose Amazon links do not match the `/dp/[A-Z0-9]{10}` pattern. No post should ever be committed with `amazon.com/s?k=` links. PA-API deprecated 2026-04-30 — Creators API requires 3 qualifying sales first, which requires real ASIN links.
