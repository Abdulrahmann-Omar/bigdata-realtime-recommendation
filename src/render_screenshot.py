"""Render text logs/output as styled HTML and screenshot with Playwright.
Usage: python src/render_screenshot.py <title> <text_file_or_-> <out.png>
Reads stdin if text_file is '-'.
"""
import sys, os
from playwright.sync_api import sync_playwright

def render(title, text, out_path):
    html = f"""<!DOCTYPE html><html><head>
<meta charset="utf-8">
<style>
  body {{ background:#1e1e1e; color:#d4d4d4; font-family:'Courier New',monospace;
         font-size:14px; padding:24px; margin:0; min-height:100vh; }}
  h2   {{ color:#4ec9b0; margin:0 0 16px 0; font-size:16px; }}
  pre  {{ background:#0d1117; border:1px solid #30363d; border-radius:8px;
         padding:20px; white-space:pre-wrap; word-break:break-all;
         line-height:1.5; overflow:auto; }}
  .ok  {{ color:#4ec9b0; }}
  .err {{ color:#f44747; }}
</style>
</head><body>
<h2>&#128202; {title}</h2>
<pre>{text.replace('<','&lt;').replace('>','&gt;')}</pre>
</body></html>"""

    tmp = "/tmp/_screenshot_source.html"
    with open(tmp, "w") as f:
        f.write(html)
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with sync_playwright() as p:
        b = p.chromium.launch()
        page = b.new_page(viewport={"width": 900, "height": 600})
        page.goto(f"file://{tmp}", wait_until="load")
        page.screenshot(path=out_path, full_page=True)
        b.close()
    print(f"saved {out_path}")

if __name__ == "__main__":
    title, src, out = sys.argv[1], sys.argv[2], sys.argv[3]
    text = sys.stdin.read() if src == "-" else open(src).read()
    render(title, text, out)
