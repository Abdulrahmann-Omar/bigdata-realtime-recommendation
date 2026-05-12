"""Render terminal-style output as a realistic Ubuntu terminal screenshot."""
import sys, os
from playwright.sync_api import sync_playwright

USER     = "abdulrahman"
HOST     = "zewail-bigdata"
PROJ_DIR = "~/Desktop/a Zewail City2/4Y/Y4S2/BigData/MiniPrj-3"

def render(title, text, out_path, show_prompt=True):
    lines_html = ""
    for line in text.split("\n"):
        esc = line.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
        # colour key output lines
        if any(k in line for k in ["ERROR","Exception","Traceback","ALERT"]):
            cl = "#f44747"
        elif any(k in line for k in ["RMSE","model saved","valid ratings","BASELINE","TUNED","Starting","saved ","Ready","done","Batch:","TRENDING","WARN"]):
            cl = "#4ec9b0"
        elif line.startswith("$") or line.startswith(">>>"):
            cl = "#9cdcfe"
        elif line.startswith("#") or line.startswith("//"):
            cl = "#6a9955"
        elif any(k in line for k in ["=====","─────","-----","━━━━"]):
            cl = "#569cd6"
        else:
            cl = "#d4d4d4"
        lines_html += f'<span style="color:{cl}">{esc}</span>\n'

    prompt_html = ""
    if show_prompt:
        prompt_html = (
            f'<div class="prompt">'
            f'<span class="user">{USER}@{HOST}</span>'
            f'<span class="sep">:</span>'
            f'<span class="path">{PROJ_DIR}</span>'
            f'<span class="dollar">$ </span>'
            f'</div>'
        )

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    background: #2d2d2d;
    font-family: 'Liberation Mono', 'DejaVu Sans Mono', 'Courier New', monospace;
    font-size: 13.5px;
    padding: 0;
  }}
  .window {{
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0 8px 32px rgba(0,0,0,0.6);
    margin: 16px;
  }}
  .titlebar {{
    background: #3c3c3c;
    padding: 8px 12px;
    display: flex;
    align-items: center;
    gap: 8px;
    border-bottom: 1px solid #1a1a1a;
  }}
  .btn {{
    width: 13px; height: 13px;
    border-radius: 50%;
    display: inline-block;
    flex-shrink: 0;
  }}
  .close  {{ background: #ff5f57; }}
  .min    {{ background: #ffbd2e; }}
  .max    {{ background: #28ca41; }}
  .title  {{
    flex: 1; text-align: center;
    color: #bbb; font-size: 12px;
    font-family: 'Liberation Sans', Arial, sans-serif;
    margin-right: 34px;
  }}
  .terminal {{
    background: #1e1e1e;
    padding: 14px 16px 18px;
    min-height: 120px;
    line-height: 1.55;
    color: #d4d4d4;
  }}
  .prompt {{
    margin-bottom: 6px;
    font-weight: bold;
  }}
  .user   {{ color: #4ec9b0; }}
  .sep    {{ color: #d4d4d4; }}
  .path   {{ color: #569cd6; }}
  .dollar {{ color: #d4d4d4; }}
  pre {{ white-space: pre-wrap; word-break: break-all; }}
</style>
</head><body>
<div class="window">
  <div class="titlebar">
    <span class="btn close"></span>
    <span class="btn min"></span>
    <span class="btn max"></span>
    <span class="title">{title} — bash</span>
  </div>
  <div class="terminal">
    {prompt_html}
    <pre>{lines_html}</pre>
  </div>
</div>
</body></html>"""

    tmp = "/tmp/_term_shot.html"
    with open(tmp, "w") as f:
        f.write(html)
    os.makedirs(os.path.dirname(out_path) if os.path.dirname(out_path) else ".", exist_ok=True)
    with sync_playwright() as p:
        b = p.chromium.launch()
        page = b.new_page(viewport={"width": 960, "height": 720})
        page.goto(f"file://{tmp}", wait_until="load")
        page.screenshot(path=out_path, full_page=True)
        b.close()
    print(f"saved {out_path}")

if __name__ == "__main__":
    title, src, out = sys.argv[1], sys.argv[2], sys.argv[3]
    text = sys.stdin.read() if src == "-" else open(src).read()
    render(title, text, out)
