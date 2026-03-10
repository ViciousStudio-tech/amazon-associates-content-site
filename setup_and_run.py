#!/usr/bin/env python3
"""
setup_and_run.py
First-time setup checker + first pipeline run.
Run this ONCE after you've filled in your .env file.
"""

import os, sys, subprocess
from pathlib import Path

def check(label, condition, fix=None):
    if condition:
        print(f"  ✓ {label}")
        return True
    else:
        print(f"  ✗ {label}")
        if fix:
            print(f"    → {fix}")
        return False

def run(cmd, capture=True):
    result = subprocess.run(cmd, shell=True, capture_output=capture, text=True)
    return result.returncode == 0, result.stdout, result.stderr

print("\n" + "═" * 55)
print("   BUSINESS 1 — FIRST-TIME SETUP CHECKER")
print("═" * 55 + "\n")

all_ok = True

# 1. Check .env exists
print("1. Checking .env file...")
env_ok = check(".env file exists", Path(".env").exists(),
               "Copy .env.template to .env and fill in your real values")
if not env_ok:
    print("\nCannot continue without .env. Please create it first.")
    sys.exit(1)

from dotenv import load_dotenv
load_dotenv()

# 2. Check required env vars
print("\n2. Checking required credentials...")
required = [
    ("ANTHROPIC_API_KEY", "console.anthropic.com"),
    ("AMAZON_ASSOCIATE_TAG", "affiliate-program.amazon.com"),
    ("GITHUB_TOKEN", "github.com → Settings → Developer settings"),
    ("GITHUB_REPO", "Format: yourusername/yourrepo"),
]
for var, source in required:
    val = os.getenv(var, "")
    ok = bool(val) and "YOUR_" not in val and "xxxx" not in val.lower()
    all_ok = check(f"{var} is set", ok, f"Get from: {source}") and all_ok

# Optional but helpful
print("\n3. Checking optional credentials (can add later)...")
optional = [
    ("GMAIL_APP_PASSWORD", "myaccount.google.com → Security → App Passwords"),
    ("AMAZON_ACCESS_KEY", "Associates dashboard → PA-API"),
]
for var, source in optional:
    val = os.getenv(var, "")
    ok = bool(val) and "YOUR_" not in val
    check(f"{var} (optional)", ok, f"Get from: {source}")

# 3. Check Python packages
print("\n4. Checking Python packages...")
ok, _, _ = run("python -c 'import anthropic, requests, dotenv'")
if not ok:
    print("  Installing packages...")
    run("pip install -r requirements.txt", capture=False)
    ok, _, _ = run("python -c 'import anthropic, requests, dotenv'")
all_ok = check("All packages installed", ok, "Run: pip install -r requirements.txt") and all_ok

# 4. Check data directory
print("\n5. Checking directories...")
for d in ["./data", "./logs", "./_posts"]:
    Path(d).mkdir(exist_ok=True)
    check(f"{d}/ directory exists", True)

print("\n" + "─" * 55)
if not all_ok:
    print("⚠  Some required items are missing. Fix them then re-run this script.")
    sys.exit(1)

print("✓ All checks passed! Running first pipeline...\n")

# Run the pipeline
steps = [
    ("Finding keywords", "python b1_keyword_finder.py"),
    ("Writing articles", "python b1_article_writer.py"),
    ("Publishing to GitHub", "python b1_publisher.py"),
]

for label, cmd in steps:
    print(f"\n{'─'*40}")
    print(f"▶ {label}...")
    print("─" * 40)
    ok, stdout, stderr = run(cmd, capture=False)
    if not ok:
        print(f"\n✗ {label} failed.")
        print("  Paste the error above into Claude for a fix.")
        sys.exit(1)

print("\n" + "═" * 55)
print("  🎉  FIRST PIPELINE RUN COMPLETE!")
print("═" * 55)
repo = os.getenv("GITHUB_REPO", "yourusername/yourrepo")
username = repo.split("/")[0] if "/" in repo else repo
repo_name = repo.split("/")[1] if "/" in repo else "site"
print(f"\n  Your site: https://{username}.github.io/{repo_name}")
print(f"  Articles:  Check your _posts/ folder")
print(f"  Logs:      Check your logs/ folder")
print(f"\n  GitHub Actions will now run this automatically every Sunday.")
print(f"  Set these secrets in GitHub → Settings → Secrets:")
print(f"    ANTHROPIC_API_KEY, AMAZON_ASSOCIATE_TAG, GH_PAT, GMAIL_APP_PASSWORD, GMAIL_TO")
print()
