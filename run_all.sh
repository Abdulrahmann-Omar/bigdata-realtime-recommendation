#!/usr/bin/env bash
# Convenience launcher — set up env and run components in order.
# Usage:
#   ./run_all.sh kafka      # start Kafka broker (foreground)
#   ./run_all.sh topic      # create the ratings-stream topic with 2 partitions
#   ./run_all.sh train      # one-shot ALS training
#   ./run_all.sh producer   # start the Kafka producer
#   ./run_all.sh stream     # start the Spark Structured Streaming app
#   ./run_all.sh dashboard  # start the Streamlit dashboard
#   ./run_all.sh screenshot # capture browser screenshots via Playwright
set -euo pipefail

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"

PROJ_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJ_DIR"

if [ -f .venv/bin/activate ]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

KAFKA_HOME="${KAFKA_HOME:-$HOME/kafka}"
KAFKA_PKG="org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"

case "${1:-}" in
  kafka)
    exec "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_HOME/config/kraft/server.properties"
    ;;
  topic)
    "$KAFKA_HOME/bin/kafka-topics.sh" --create --if-not-exists \
        --topic ratings-stream --bootstrap-server localhost:9092 \
        --partitions 2 --replication-factor 1
    "$KAFKA_HOME/bin/kafka-topics.sh" --describe --topic ratings-stream \
        --bootstrap-server localhost:9092
    ;;
  train)
    spark-submit --master 'local[*]' --driver-memory 4g src/train_als.py
    ;;
  producer)
    exec python src/producer.py
    ;;
  stream)
    exec spark-submit --master 'local[*]' --driver-memory 4g \
        --packages "$KAFKA_PKG" src/stream_app.py
    ;;
  dashboard)
    exec streamlit run src/dashboard.py
    ;;
  screenshot)
    exec python src/screenshot.py
    ;;
  *)
    echo "Usage: $0 {kafka|topic|train|producer|stream|dashboard|screenshot}" >&2
    exit 2
    ;;
esac
