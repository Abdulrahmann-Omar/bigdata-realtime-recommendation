"""
Automated pipeline runner + screenshot capture.
Run AFTER Kafka is installed and the broker is started.

Usage:
  cd /path/to/MiniPrj-3
  source .venv/bin/activate
  python src/run_pipeline_and_screenshot.py

What it does:
  1. Formats KRaft storage + starts Kafka broker (subprocess)
  2. Creates ratings-stream topic (2 partitions)
  3. Screenshots kafka-topics --describe output
  4. Starts producer subprocess
  5. Starts stream_app in subprocess
  6. Waits for streaming console output to accumulate
  7. Screenshots: streaming batches, Spark UI, dashboard
  8. Stops everything cleanly
"""
import os, sys, subprocess, time, re
from pathlib import Path
from playwright.sync_api import sync_playwright

BASE   = Path(__file__).resolve().parent.parent
KAFKA  = Path.home() / "kafka"
VENV   = BASE / ".venv"
PY     = VENV / "bin" / "python"
SPARK  = VENV / "lib/python3.13/site-packages/pyspark/bin/spark-submit"
SHOTS  = BASE / "screenshots"
LOGS   = BASE / "logs"
SHOTS.mkdir(exist_ok=True)
LOGS.mkdir(exist_ok=True)

os.environ["JAVA_HOME"]  = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["PATH"]       = f"/usr/lib/jvm/java-17-openjdk-amd64/bin:{os.environ['PATH']}"
os.environ["KAFKA_HOME"] = str(KAFKA)

KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
BOOTSTRAP = "localhost:9092"
TOPIC     = "ratings-stream"


def sh(cmd, capture=True, **kw):
    r = subprocess.run(cmd, shell=True, capture_output=capture,
                       text=True, cwd=BASE, **kw)
    return r.stdout.strip() if capture else r


def render_text(title, text, out):
    sys.path.insert(0, str(BASE / "src"))
    from render_screenshot import render
    render(title, str(text), str(out))


def browser_shot(url, out, wait_ms=3000):
    with sync_playwright() as p:
        b = p.chromium.launch()
        page = b.new_page(viewport={"width": 1600, "height": 900})
        try:
            page.goto(url, wait_until="networkidle", timeout=20000)
            page.wait_for_timeout(wait_ms)
            page.screenshot(path=str(out), full_page=True)
            print(f"  saved {out}")
        except Exception as e:
            print(f"  skip {url}: {e}", file=sys.stderr)
        b.close()


# ── 1. Format KRaft storage (idempotent check) ──────────────────────────────
meta_log = KAFKA / "data" / "meta.properties"
if not meta_log.exists():
    print("[1] Formatting KRaft storage…")
    cluster_id = sh(f"{KAFKA}/bin/kafka-storage.sh random-uuid")
    sh(f"{KAFKA}/bin/kafka-storage.sh format -t {cluster_id} "
       f"-c {KAFKA}/config/kraft/server.properties", capture=False)
else:
    print("[1] KRaft already formatted, skipping.")

# ── 2. Start broker ──────────────────────────────────────────────────────────
print("[2] Starting Kafka broker…")
broker_log = LOGS / "kafka_broker.log"
broker = subprocess.Popen(
    [f"{KAFKA}/bin/kafka-server-start.sh",
     f"{KAFKA}/config/kraft/server.properties"],
    stdout=open(broker_log, "w"), stderr=subprocess.STDOUT, cwd=BASE)
print(f"    broker pid={broker.pid}, log={broker_log}")

# Wait for broker ready
for _ in range(30):
    time.sleep(2)
    out = sh(f"{KAFKA}/bin/kafka-topics.sh --list --bootstrap-server {BOOTSTRAP} 2>&1")
    if "ERROR" not in out and "Connection refused" not in out:
        print("    broker ready!")
        break

# ── 3. Create topic ──────────────────────────────────────────────────────────
print("[3] Creating topic…")
sh(f"{KAFKA}/bin/kafka-topics.sh --create --if-not-exists "
   f"--topic {TOPIC} --bootstrap-server {BOOTSTRAP} "
   f"--partitions 2 --replication-factor 1", capture=False)

describe = sh(f"{KAFKA}/bin/kafka-topics.sh --describe "
              f"--topic {TOPIC} --bootstrap-server {BOOTSTRAP}")
print(describe)
render_text("Kafka Topic — ratings-stream (2 partitions)",
            f"$ kafka-topics.sh --describe --topic {TOPIC}\n\n{describe}",
            SHOTS / "11_kafka_topic_created.png")

# ── 4. Start producer ────────────────────────────────────────────────────────
print("[4] Starting producer…")
prod_log = LOGS / "producer.log"
producer = subprocess.Popen(
    [str(PY), "src/producer.py"],
    stdout=open(prod_log, "w"), stderr=subprocess.STDOUT, cwd=BASE)
print(f"    producer pid={producer.pid}")
time.sleep(3)

# ── 5. Start streaming app ───────────────────────────────────────────────────
print("[5] Starting streaming app…")
stream_log = LOGS / "stream_app.log"
stream = subprocess.Popen(
    [str(SPARK), "--master", "local[4]", "--driver-memory", "4g",
     "--packages", KAFKA_PKG, "src/stream_app.py"],
    stdout=open(stream_log, "w"), stderr=subprocess.STDOUT, cwd=BASE)
print(f"    stream pid={stream.pid}, log={stream_log}")

# ── 6. Wait for first batches ────────────────────────────────────────────────
print("[6] Waiting for streaming batches (up to 3 min)…")
deadline = time.time() + 180
batch_text = ""
while time.time() < deadline:
    time.sleep(10)
    if stream_log.exists():
        content = stream_log.read_text()
        if "Batch:" in content:
            # Extract last few batches
            batches = re.findall(r"(-{40,}\nBatch: \d+\n-{40,}.*?)(?=-{40,}\nBatch:|\Z)",
                                 content, re.DOTALL)
            if batches:
                batch_text = "\n".join(batches[-3:])[:3000]
                print(f"    got batch data ({len(batch_text)} chars)")
                break

if batch_text:
    render_text("Spark Structured Streaming — Batch Output",
                batch_text, SHOTS / "12_streaming_batches.png")

# Grab lag line
lag_lines = [l for l in stream_log.read_text().split("\n") if "avg end-to-end lag" in l]
if lag_lines:
    render_text("End-to-End Latency Measurement",
                "\n".join(lag_lines[-5:]), SHOTS / "13_latency.png")

# ── 7. Alert output ──────────────────────────────────────────────────────────
alert_lines = [l for l in stream_log.read_text().split("\n") if "TRENDING" in l]
if alert_lines:
    render_text("TRENDING Alerts (avg_rating > 4.5)",
                "\n".join(alert_lines[-20:]), SHOTS / "14_alerts.png")

# ── 8. Spark UI screenshot ────────────────────────────────────────────────────
print("[8] Screenshotting Spark UI…")
browser_shot("http://localhost:4040/StreamingQuery/", SHOTS / "15_spark_ui_streaming.png")
browser_shot("http://localhost:4040/jobs/", SHOTS / "16_spark_ui_jobs.png")

# ── 9. Start dashboard + screenshot ─────────────────────────────────────────
print("[9] Starting Streamlit dashboard…")
dash_log = LOGS / "dashboard.log"
dashboard = subprocess.Popen(
    [str(PY), "-m", "streamlit", "run", "src/dashboard.py",
     "--server.headless", "true", "--server.port", "8501"],
    stdout=open(dash_log, "w"), stderr=subprocess.STDOUT, cwd=BASE)
time.sleep(8)
browser_shot("http://localhost:8501", SHOTS / "17_dashboard.png", wait_ms=5000)

# ── 10. Let it run a bit more then stop cleanly ──────────────────────────────
print("[10] Running for 30s more to accumulate data…")
time.sleep(30)

# Refresh screenshots
browser_shot("http://localhost:8501", SHOTS / "17_dashboard_full.png", wait_ms=5000)
browser_shot("http://localhost:4040/StreamingQuery/", SHOTS / "15_spark_ui_streaming2.png")

print("[11] Stopping all processes…")
for proc in [producer, stream, dashboard, broker]:
    try:
        proc.terminate()
        proc.wait(timeout=10)
    except Exception:
        proc.kill()

print("\nAll done! Screenshots saved to screenshots/")
for f in sorted(SHOTS.glob("*.png")):
    print(f"  {f.name}")
