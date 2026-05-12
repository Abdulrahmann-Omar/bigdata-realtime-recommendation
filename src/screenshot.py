"""Capture Spark UI + Streamlit dashboard screenshots via Playwright."""
import os, sys
from playwright.sync_api import sync_playwright

TARGETS = [
    ("http://localhost:4040/",                         "screenshots/01_spark_ui_jobs.png"),
    ("http://localhost:4040/StreamingQuery/",          "screenshots/02_spark_ui_streaming.png"),
    ("http://localhost:8501/",                         "screenshots/03_dashboard.png"),
]

os.makedirs("screenshots", exist_ok=True)
with sync_playwright() as p:
    b = p.chromium.launch()
    for url, out in TARGETS:
        try:
            page = b.new_page(viewport={"width": 1600, "height": 1000})
            page.goto(url, wait_until="networkidle", timeout=30000)
            page.wait_for_timeout(2500)
            page.screenshot(path=out, full_page=True)
            print(f"saved {out}")
        except Exception as e:
            print(f"skip {url}: {e}", file=sys.stderr)
        finally:
            try: page.close()
            except: pass
    b.close()
