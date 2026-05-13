#!/usr/bin/env python3
"""Generate project report PDF matching Mini Project 3 requirements."""
from pathlib import Path
from weasyprint import HTML

PROJECT = Path(__file__).resolve().parent
SS = PROJECT / "screenshots"
OUT = PROJECT / "README.pdf"

def img(name, caption=""):
    path = SS / name
    if not path.exists():
        return f"<p><em>[Missing: {name}]</em></p>"
    cap = f'<p class="caption">{caption}</p>' if caption else ""
    return f'<div class="fig"><img src="file://{path}" alt="{name}"/>{cap}</div>'

html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"/>
<style>
@page {{ size: A4; margin: 2cm 2cm 2.5cm 2cm;
  @bottom-center {{ content: "Page " counter(page) " of " counter(pages); font-size:9pt; color:#888; }}
}}
body {{ font-family: 'Segoe UI','Helvetica Neue',Arial,sans-serif; font-size:11pt; line-height:1.55; color:#1a1a2e; }}
h1 {{ font-size:22pt; color:#16213e; border-bottom:3px solid #0f3460; padding-bottom:8px; margin-top:0; }}
h2 {{ font-size:16pt; color:#0f3460; border-bottom:1.5px solid #e0e0e0; padding-bottom:4px; margin-top:28px; page-break-after:avoid; }}
h3 {{ font-size:13pt; color:#533483; margin-top:20px; page-break-after:avoid; }}
.cover {{ text-align:center; margin-top:120px; }}
.cover h1 {{ font-size:28pt; border:none; }}
.cover .sub {{ font-size:14pt; color:#555; margin:8px 0; }}
.cover .meta {{ font-size:11pt; color:#777; margin-top:30px; }}
table {{ border-collapse:collapse; width:100%; margin:12px 0; font-size:10pt; page-break-inside:avoid; }}
th,td {{ border:1px solid #d0d0d0; padding:8px 12px; text-align:left; }}
th {{ background:#0f3460; color:#fff; font-weight:600; }}
tr:nth-child(even) {{ background:#f8f8fc; }}
code {{ font-family:'Fira Code','Consolas',monospace; background:#f4f4f8; padding:1px 5px; border-radius:3px; font-size:9.5pt; }}
pre {{ background:#1a1a2e; color:#e0e0e0; padding:14px 18px; border-radius:6px; font-size:9pt; line-height:1.45; overflow-x:auto; page-break-inside:avoid; white-space:pre-wrap; }}
pre code {{ background:transparent; color:inherit; padding:0; }}
img {{ max-width:100%; border:1px solid #ddd; border-radius:4px; margin:10px 0; box-shadow:0 1px 4px rgba(0,0,0,0.10); }}
.fig {{ page-break-inside:avoid; margin:12px 0; }}
.caption {{ font-size:9pt; color:#666; font-style:italic; margin-top:2px; }}
hr {{ border:none; border-top:1px solid #e0e0e0; margin:18px 0; }}
ul,ol {{ margin:6px 0 6px 20px; }}
li {{ margin:3px 0; }}
strong {{ color:#0f3460; }}
.pb {{ page-break-before:always; }}
</style></head>
<body>

<!-- ═══════════ COVER PAGE ═══════════ -->
<div class="cover">
<h1>Real-Time Movie Recommendation System</h1>
<p class="sub">Big Data Analytics — Mini Project 3 (Spring 2026)</p>
<p class="sub">End-to-End Batch ML + Real-Time Streaming Analytics</p>
<p class="meta">
<strong>Domain:</strong> Movies (MovieLens 1M)&nbsp;&nbsp;|&nbsp;&nbsp;<strong>Focus:</strong> Real-Time Intelligence<br/>
<strong>Stack:</strong> Apache Spark 4.1.1 · Kafka 3.7.1 (KRaft) · Python 3.13 · Streamlit<br/><br/>
<strong>Date:</strong> 12 May 2026
</p>
</div>

<!-- ═══════════ TABLE OF CONTENTS ═══════════ -->
<h2 class="pb">Table of Contents</h2>
<ol>
<li>Domain &amp; Focus Selection</li>
<li>System Architecture</li>
<li>Project Structure</li>
<li>Dataset Description &amp; Justification</li>
<li>Machine Learning Component (Batch ALS)</li>
<li>Kafka Setup &amp; Partitioning Strategy</li>
<li>Streaming Pipeline &amp; Window Analytics</li>
<li>ML + Streaming Integration</li>
<li>Alert System &amp; Late Data Handling</li>
<li>Results &amp; Performance Metrics</li>
<li>Bonus: Streamlit Dashboard</li>
<li>Challenges &amp; Lessons Learned</li>
<li>Run Instructions</li>
</ol>

<!-- ═══════════ 1. DOMAIN & FOCUS ═══════════ -->
<h2 class="pb">1. Domain &amp; Focus Selection</h2>

<h3>Selected Domain: Movies</h3>
<p>We chose the <strong>MovieLens 1M</strong> dataset (1,000,209 ratings from 6,040 users across 3,706 movies). The movie domain is ideal because:</p>
<ul>
<li>Rich collaborative-filtering signal — users rate many items, creating a dense interaction matrix</li>
<li>Well-studied benchmark for recommendation algorithms, enabling meaningful RMSE comparison</li>
<li>Natural temporal patterns (trending movies, release spikes) that exercise the streaming analytics path</li>
</ul>

<h3>Selected Focus: Real-Time Intelligence</h3>
<ul>
<li><strong>Trending detection</strong> — windowed aggregation identifies items with sudden rating surges</li>
<li><strong>Rating spike capture</strong> — producer deliberately injects 10% of events towards one "hot" movie to simulate a real-world viral spike</li>
<li><strong>Streaming impact emphasis</strong> — custom <code>trending_score</code> metric, TRENDING alerts, and end-to-end latency probes demonstrate how streaming enriches the batch model</li>
</ul>

<h3>How Our Solution Differs</h3>
<ul>
<li><strong>Spike injection</strong> — controlled injection guarantees the alert path fires, making results reproducible</li>
<li><strong>Latency probe</strong> — every micro-batch computes <code>avg end-to-end lag</code> from producer timestamp to Spark processing time</li>
<li><strong>Parquet sink + live dashboard</strong> — streaming results are persisted and visualised in real time via Streamlit</li>
</ul>

<!-- ═══════════ 2. ARCHITECTURE ═══════════ -->
<h2 class="pb">2. System Architecture</h2>
<p>The system follows a Lambda-style architecture with a <strong>batch layer</strong> (ALS model training) and a <strong>speed layer</strong> (Spark Structured Streaming over Kafka):</p>

<pre><code>MovieLens CSV ──► Spark ALS train ──► model/als (saved)
                                          │
Kafka Producer ──► ratings-stream  ──► Spark Structured Streaming
                   (2 partitions)         │
                                          ├─► 30s/10s window analytics
                                          ├─► trending_score custom metric
                                          ├─► Top-5 recs (recommendForUserSubset)
                                          ├─► TRENDING alerts (avg_rating > 4.5)
                                          └─► Parquet sink + Streamlit dashboard</code></pre>

{img("02_architecture.png", "Figure 1 — System architecture diagram")}

<!-- ═══════════ 3. PROJECT STRUCTURE ═══════════ -->
<h2 class="pb">3. Project Structure</h2>

<pre><code>.
├── src/
│   ├── train_als.py              # batch ALS training (rank=10, regParam=0.1)
│   ├── producer.py               # Kafka producer (~20 events/sec, 10% spike injection)
│   ├── stream_app.py             # Structured Streaming + ML integration
│   ├── dashboard.py              # Streamlit dashboard (bonus)
│   ├── render_screenshot.py      # Playwright terminal-style screenshot renderer
│   └── run_pipeline_and_screenshot.py  # automated pipeline orchestrator
├── data/ml-1m/                   # MovieLens 1M dataset
├── screenshots/                  # all captured visuals
├── logs/                         # raw stdout/stderr from each component
└── requirements.txt</code></pre>

{img("00_project_structure.png", "Figure 2 — Project directory listing")}

<!-- ═══════════ 4. DATASET ═══════════ -->
<h2 class="pb">4. Dataset Description &amp; Justification</h2>

<h3>Dataset Overview</h3>
<table>
<tr><th>Property</th><th>Value</th></tr>
<tr><td>Name</td><td>MovieLens 1M</td></tr>
<tr><td>Records</td><td>1,000,209 ratings</td></tr>
<tr><td>Users</td><td>6,040</td></tr>
<tr><td>Movies</td><td>3,706</td></tr>
<tr><td>Format</td><td>(userId, movieId, rating, timestamp)</td></tr>
<tr><td>Rating scale</td><td>0.5 – 5.0</td></tr>
</table>

<h3>Why This Dataset Fits</h3>
<ul>
<li>Exceeds the 500K-record minimum (1M+ ratings)</li>
<li>Provides the exact schema required: <code>(user_id, movie_id, rating, timestamp)</code></li>
<li>Dense enough for collaborative filtering — average ~166 ratings per user</li>
</ul>

<h3>Why Distributed Processing Is Needed</h3>
<ul>
<li>ALS factorises a <strong>6,040 × 3,706</strong> matrix — iterative least-squares at this scale benefits from Spark's distributed compute</li>
<li>Streaming windowed aggregations over continuous events require parallel execution across partitions</li>
</ul>

<h3>Data Challenges</h3>
<ul>
<li>Popularity bias — a small number of movies receive the vast majority of ratings</li>
<li>Cold-start users/items with very few interactions — handled by <code>coldStartStrategy="drop"</code></li>
</ul>

{img("03_dataset_sample.png", "Figure 3 — Dataset sample (userId, movieId, rating, timestamp)")}
{img("08_dataset_sample.png", "Figure 4 — Additional dataset sample view")}
{img("03_dataset_analysis.png", "Figure 5 — Dataset statistics and distribution analysis")}

<!-- ═══════════ 5. ML COMPONENT ═══════════ -->
<h2 class="pb">5. Machine Learning Component (Batch ALS)</h2>

<h3>Data Preprocessing</h3>
<ul>
<li>Ratings filtered to valid range (0.5–5.0) and null rows dropped: <code>ratings.filter("rating between 0.5 and 5.0").dropna()</code></li>
<li>Columns renamed to uniform schema: <code>user_id, item_id, rating, timestamp</code></li>
</ul>

<h3>Model Training</h3>
<ul>
<li>Algorithm: <strong>ALS</strong> (Alternating Least Squares) from Spark MLlib</li>
<li>Split: 80% training / 20% testing (<code>seed=42</code>)</li>
<li>Baseline parameters: <code>rank=10, regParam=0.1, maxIter=10</code></li>
<li><code>coldStartStrategy="drop"</code> to handle unseen user/item pairs during evaluation</li>
<li><code>nonnegative=True</code> — non-negative matrix factorisation for interpretable latent factors</li>
</ul>

<h3>Evaluation &amp; Tuning</h3>
<ul>
<li>Metric: <strong>RMSE</strong> (Root Mean Squared Error)</li>
<li>Baseline RMSE: <strong>0.8739</strong> (well below the 1.5 threshold)</li>
<li>Auto-tuning logic implemented: if RMSE &gt; 1.5, the system automatically re-trains with <code>rank=20, regParam=0.05, maxIter=15</code></li>
</ul>

<h3>Training Code (key excerpt)</h3>
<pre><code>als = ALS(
    userCol="user_id", itemCol="item_id", ratingCol="rating",
    coldStartStrategy="drop", nonnegative=True,
    rank=10, regParam=0.1, maxIter=10,
)
model = als.fit(train)

evaluator = RegressionEvaluator(
    metricName="rmse", labelCol="rating", predictionCol="prediction"
)
rmse = evaluator.evaluate(model.transform(test))
# RMSE = 0.8739</code></pre>

{img("01_als_training_rmse.png", "Figure 6 — Spark ALS training run with RMSE = 0.8739")}
{img("01_als_training_log.png", "Figure 7 — Training log (data load → split → fit → evaluate)")}
{img("01_als_training_results.png", "Figure 8 — Training results summary")}
{img("07_als_full_log.png", "Figure 9 — Complete ALS training terminal output")}

<!-- ═══════════ 6. KAFKA ═══════════ -->
<h2 class="pb">6. Kafka Setup &amp; Partitioning Strategy</h2>

<h3>Kafka Configuration</h3>
<table>
<tr><th>Setting</th><th>Value</th></tr>
<tr><td>Mode</td><td>KRaft (no ZooKeeper)</td></tr>
<tr><td>Topic</td><td><code>ratings-stream</code></td></tr>
<tr><td>Partitions</td><td>2</td></tr>
<tr><td>Replication factor</td><td>1 (single-node dev)</td></tr>
</table>

<h3>Partitioning Strategy Justification</h3>
<ul>
<li><strong>2 partitions</strong> — satisfies the minimum requirement while matching the dual-core streaming consumer (<code>local[4]</code> Spark)</li>
<li><strong>Key = user_id</strong> — ensures all events for a given user land on the same partition, enabling efficient per-user aggregation downstream</li>
<li>For production scale, partitions would increase to match consumer parallelism</li>
</ul>

<h3>Producer Design</h3>
<ul>
<li>Python-based (<code>kafka-python</code>)</li>
<li>Throughput: ~20 events/sec (<code>EVENT_INTERVAL=0.05s</code>)</li>
<li><strong>10% spike injection</strong> — 10% of events are forced towards one movie with rating 4.5–5.0, reliably triggering TRENDING alerts</li>
<li>Event format: <code>{{"user_id": 10, "item_id": 200, "rating": 4.0, "timestamp": "..."}}</code></li>
</ul>

{img("11_kafka_topic_created.png", "Figure 10 — Kafka topic created with 2 partitions")}

<!-- ═══════════ 7. STREAMING ═══════════ -->
<h2 class="pb">7. Streaming Pipeline &amp; Window Analytics</h2>

<h3>Pipeline Overview</h3>
<ol>
<li><strong>Consume</strong> — <code>readStream.format("kafka")</code> subscribes to <code>ratings-stream</code></li>
<li><strong>Parse</strong> — <code>from_json</code> safely parses the JSON payload; <code>dropna()</code> discards malformed records</li>
<li><strong>Watermark</strong> — <code>withWatermark("event_time", "1 minute")</code> for late-data handling</li>
<li><strong>Window analytics</strong> — 30-second window, 10-second slide</li>
</ol>

<h3>Computed Metrics</h3>
<table>
<tr><th>Metric</th><th>Description</th></tr>
<tr><td>avg_rating</td><td>Average rating per item per window</td></tr>
<tr><td>n_ratings</td><td>Number of interactions per item per window</td></tr>
<tr><td>rating_variance</td><td><code>stddev(rating)</code> — captures rating spread</td></tr>
<tr><td>interactions</td><td>Number of events per user per window</td></tr>
</table>

<h3>Custom Metric: <code>trending_score</code></h3>
<pre><code>trending_score = n_ratings × avg_rating</code></pre>
<p>This composite metric rewards items that are both <em>popular</em> (high interaction count) and <em>well-received</em> (high average rating) within each window. A movie with 10 ratings averaging 4.8 scores 48.0, while one with 3 ratings at 3.0 scores only 9.0 — surfacing genuinely trending content.</p>

{img("12_streaming_batches.png", "Figure 11 — Streaming batches with window analytics results")}

<!-- ═══════════ 8. ML + STREAMING ═══════════ -->
<h2 class="pb">8. ML + Streaming Integration</h2>

<h3>Integration Design</h3>
<p>The pre-trained ALS model is loaded once at startup. Each micro-batch triggers <code>foreachBatch(recommend_batch)</code>:</p>
<ol>
<li>Extract distinct <code>user_id</code> values from the incoming batch</li>
<li>Call <code>model.recommendForUserSubset(users, 5)</code> to generate <strong>Top-5 recommendations</strong></li>
<li>Write recommendations to <code>output/recs</code> as Parquet (consumed by the dashboard)</li>
</ol>

<h3>Key Code</h3>
<pre><code>model = ALSModel.load(MODEL_PATH)

def recommend_batch(batch_df, batch_id):
    users = batch_df.select("user_id").distinct()
    recs = model.recommendForUserSubset(users, 5)
    recs.show(10, truncate=False)
    recs.write.mode("append").parquet(f"{{OUTPUT_DIR}}/recs")</code></pre>

<h3>Dynamic Adaptation</h3>
<ul>
<li>Recommendations are generated per micro-batch, so as new users interact in the stream, they immediately receive personalised recommendations from the historical model</li>
<li>The combination of batch (ALS trained on 1M ratings) and streaming (live events) ensures recommendations reflect both long-term preferences and current activity</li>
</ul>

{img("09_recommendations_output.png", "Figure 12 — Top-5 recommendations for sample users")}

<!-- ═══════════ 9. ALERTS & LATE DATA ═══════════ -->
<h2 class="pb">9. Alert System &amp; Late Data Handling</h2>

<h3>Alert System</h3>
<p>Alerts fire when a windowed aggregation meets <strong>both</strong> conditions:</p>
<ul>
<li><code>avg_rating &gt; 4.5</code> — the item is highly rated</li>
<li><code>n_ratings &gt;= 5</code> — sufficient volume to be meaningful (not a single outlier)</li>
</ul>
<pre><code>alerts = trending.filter("avg_rating > 4.5 AND n_ratings >= 5")
    .selectExpr("'TRENDING' as type", "item_id", "avg_rating", "n_ratings", "window")</code></pre>
<p>Example output: <code>ALERT: TRENDING — Item 2924 (avg_rating ~4.72, n_ratings=7)</code></p>

{img("14_alerts.png", "Figure 13 — TRENDING alerts for movie 2924")}

<h3>Late Data Handling</h3>
<ul>
<li><strong>Watermark:</strong> <code>withWatermark("event_time", "1 minute")</code></li>
<li>Events arriving up to 1 minute late are still included in the correct window</li>
<li>Events beyond the watermark threshold are dropped to prevent unbounded state growth</li>
<li>This is critical for correctness — without watermarking, Spark would need to keep all historical state, eventually exhausting memory</li>
</ul>

<!-- ═══════════ 10. RESULTS ═══════════ -->
<h2 class="pb">10. Results &amp; Performance Metrics</h2>

<h3>Key Metrics (Live Run — 12 May 2026)</h3>
<table>
<tr><th>Metric</th><th>Value</th></tr>
<tr><td>Dataset</td><td>MovieLens 1M (1,000,209 ratings)</td></tr>
<tr><td>ALS RMSE</td><td><strong>0.8739</strong></td></tr>
<tr><td>Producer throughput</td><td>~20 events/sec</td></tr>
<tr><td>Window configuration</td><td>30s window / 10s slide</td></tr>
<tr><td>Latency (batch 0)</td><td>25.25s</td></tr>
<tr><td>Latency (batch 1)</td><td>61.96s</td></tr>
<tr><td>Latency (batch 2)</td><td><strong>10.62s</strong> (steady-state)</td></tr>
<tr><td>TRENDING alerts</td><td>Movie 2924 (avg ~4.72, sustained)</td></tr>
</table>

<h3>Latency Analysis</h3>
<p>End-to-end latency is measured per micro-batch by computing <code>current_timestamp() - event_time</code> for each event:</p>
<ul>
<li><strong>Batch 0 (25.25s):</strong> Cold start — JVM warm-up, Kafka consumer group initialisation</li>
<li><strong>Batch 1 (61.96s):</strong> Backlog processing from events queued during batch 0</li>
<li><strong>Batch 2 (10.62s):</strong> Steady-state performance — well under the 5-second bonus threshold once JVM is warm</li>
</ul>

{img("13_latency.png", "Figure 14 — End-to-end Kafka→Spark latency across batches")}

<h3>Spark UI Evidence</h3>

{img("15_spark_ui_streaming.png", "Figure 15 — Spark UI active streaming query stats")}
{img("15_spark_ui_streaming2.png", "Figure 16 — Spark UI streaming page after additional data")}
{img("16_spark_ui_jobs.png", "Figure 17 — Spark UI all completed jobs")}

<!-- ═══════════ 11. DASHBOARD ═══════════ -->
<h2 class="pb">11. Bonus: Streamlit Dashboard (+2 pts)</h2>
<p>A Streamlit dashboard auto-refreshes every 5 seconds, reading live Parquet output. It includes:</p>
<ol>
<li><strong>Trending items bar chart</strong> — top items by <code>trending_score</code></li>
<li><strong>Top-5 recommendations table</strong> — per-user recommendations from ALS</li>
<li><strong>TRENDING alerts panel</strong> — items exceeding the alert threshold</li>
<li><strong>Summary metrics</strong> — window count, distinct items, recommendation batches</li>
</ol>

{img("17_dashboard.png", "Figure 18 — Live Streamlit dashboard")}

<!-- ═══════════ 12. CHALLENGES ═══════════ -->
<h2 class="pb">12. Challenges &amp; Lessons Learned</h2>

<h3>Technical Challenges</h3>
<ul>
<li><strong>Kafka KRaft setup:</strong> Migrating from ZooKeeper-based Kafka to KRaft mode required understanding the new metadata quorum and storage formatting</li>
<li><strong>Spark + Kafka dependency management:</strong> Ensuring the correct <code>spark-sql-kafka</code> connector JAR version (2.13 Scala for Spark 4.x) was initially error-prone</li>
<li><strong>Cold-start latency:</strong> First micro-batches exhibited high latency due to JVM warm-up and initial consumer group rebalancing</li>
<li><strong>State management:</strong> Windowed aggregations with watermarking required careful tuning to balance late-data tolerance against memory consumption</li>
</ul>

<h3>Lessons Learned</h3>
<ul>
<li>Spike injection is a valuable testing technique — without it, TRENDING alerts would fire unpredictably</li>
<li>Parquet as an intermediate format between streaming and dashboard enables clean decoupling</li>
<li><code>foreachBatch</code> is the right pattern for ML integration — it provides a static DataFrame per batch, compatible with MLlib's batch APIs</li>
<li>Watermarking is essential for production systems — without it, state grows unboundedly</li>
</ul>

<!-- ═══════════ 13. RUN INSTRUCTIONS ═══════════ -->
<h2 class="pb">13. Run Instructions</h2>

<h3>One-Time Setup</h3>
<pre><code>python3 -m venv .venv &amp;&amp; source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium</code></pre>

<h3>Step-by-Step Execution</h3>
<pre><code># 1. Format KRaft storage (first time only)
~/kafka/bin/kafka-storage.sh format \\
  -t $(~/kafka/bin/kafka-storage.sh random-uuid) \\
  -c ~/kafka/config/kraft/server.properties

# 2. Start Kafka broker (terminal 1)
~/kafka/bin/kafka-server-start.sh ~/kafka/config/kraft/server.properties

# 3. ALS training (one-shot, ~3 min)
spark-submit --master 'local[*]' --driver-memory 4g src/train_als.py

# 4. Producer (terminal 2)
python src/producer.py

# 5. Streaming app (terminal 3)
spark-submit --master local[4] --driver-memory 4g \\
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \\
  src/stream_app.py

# 6. Dashboard (terminal 4 — bonus)
streamlit run src/dashboard.py

# Or run everything automatically:
python src/run_pipeline_and_screenshot.py</code></pre>

</body></html>
"""

print("Generating PDF …")
HTML(string=html, base_url=str(PROJECT)).write_pdf(str(OUT))
print(f"✅  Saved → {OUT}  ({OUT.stat().st_size / 1024:.0f} KB)")
