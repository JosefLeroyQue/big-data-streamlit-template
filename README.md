# Weather Weather Lang

A real-time weather pipeline:

```
WeatherAPI → Kafka (KRaft) → Spark Structured Streaming → MongoDB → Streamlit dashboard
```

| Service | Folder | What it does |
|---|---|---|
| `producer` | `producer/` | Polls WeatherAPI and publishes temperature, humidity, wind speed and rainfall records to Kafka topic `streaming-data` |
| `kafka` / `kafka-init` | – | Single-node Kafka in KRaft mode; `kafka-init` creates the topic (3 partitions) before anything else starts |
| `processor` | `processor/` | Spark job: parses JSON, drops invalid records, upserts into MongoDB |
| `mongodb` | – | Stores records in `weather_db.weather_data` |
| `dashboard` | `dashboard/` | Streamlit app that reads MongoDB and shows live readings, trends and analytics |

## 1. Prerequisites

- Docker Desktop (or Docker Engine) with **Docker Compose v2** (`docker compose version`)
- A free WeatherAPI key from <https://www.weatherapi.com/>
- Internet access on first start (Docker images, Python packages, and the Spark Kafka connector jars)
- About 4 GB of free RAM

## 2. Configure

```bash
cp .env.example .env        # Windows PowerShell: Copy-Item .env.example .env
```

Edit `.env` and set `WEATHER_API_KEY`. Never commit `.env` (it is in `.gitignore`).

| Variable | Default | Used by | Meaning |
|---|---|---|---|
| `WEATHER_API_KEY` | *(required)* | producer | Your WeatherAPI key |
| `WEATHER_LOCATION` | `Manila` | producer | City or `lat,lon` passed to WeatherAPI |
| `POLL_SECONDS` | `60` | producer | Seconds between API calls |
| `KAFKA_TOPIC` | `streaming-data` | producer, processor, kafka-init | Topic name |
| `KAFKA_STARTING_OFFSETS` | `latest` | processor | `earliest` replays the topic on a fresh checkpoint |
| `MONGODB_DATABASE` | `weather_db` | processor, dashboard | Database name |
| `MONGODB_COLLECTION` | `weather_data` | processor, dashboard | Collection name |
| `DASHBOARD_TIMEZONE` | `Asia/Manila` | dashboard | Timezone used to display timestamps (stored as UTC) |
| `DASHBOARD_REFRESH_SECONDS` | `20` | dashboard | Auto-refresh interval |
| `DASHBOARD_DEMO_MODE` | `false` | dashboard | `true` shows synthetic data instead of MongoDB |

Connection addresses between containers are fixed in `compose.yaml`: Kafka at `kafka:29092` and MongoDB at `mongodb:27017`. They use service names, never `localhost`.

## 3. Start

```bash
docker compose up -d --build
docker compose ps            # wait until kafka, mongodb, processor and dashboard show "healthy"
docker compose logs -f producer processor
```

The **first start is slow** (a few minutes): the processor downloads the Spark Kafka connector jars. They are cached in the `spark-ivy` volume afterwards.

Open the dashboard: <http://localhost:8501>. The first readings appear about a minute after the producer starts; until then it shows "Waiting for weather data".

Other useful URLs: Spark UI <http://localhost:4040>, MongoDB `localhost:27017`, Kafka from your host `localhost:9092`.

Stop: `docker compose down` (keeps data) or `docker compose down -v` (deletes Kafka, MongoDB and checkpoint data).

## 4. Verify each stage

```bash
# 1. Producer is publishing
docker compose logs --tail 5 producer

# 2. Messages are in Kafka
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic streaming-data --from-beginning --max-messages 4

# 3. Spark is writing batches
docker compose logs --tail 10 processor

# 4. Records are in MongoDB
docker compose exec mongodb mongosh weather_db --quiet \
  --eval "db.weather_data.find({}, {_id:0}).sort({timestamp:-1}).limit(4)"
```

### Record format

One message/document per metric (long format):

```json
{"timestamp": "2026-10-25T02:00:00+00:00", "value": 29.4, "metric_type": "temperature",
 "sensor_id": "weatherapi_temperature", "location": "Manila", "unit": "°C",
 "country": "Philippines", "condition": "Partly cloudy", "source": "WeatherAPI"}
```

`metric_type` is one of `temperature`, `humidity`, `wind_speed`, `rainfall`. The dashboard pivots these into one row per location and timestamp. MongoDB has a unique index on `(timestamp, metric_type, location, sensor_id)` and the processor upserts, so a replayed Spark batch never creates duplicates.

## 5. Running pieces on your host (optional)

Start only the infrastructure, then run Python directly. From your host, use the *external* addresses:

```bash
docker compose up -d kafka kafka-init mongodb
python -m venv .venv && source .venv/bin/activate        # Windows: .venv\Scripts\activate

# producer
pip install -r producer/requirements.txt
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python producer/producer.py   # reads WEATHER_API_KEY from .env

# dashboard
pip install -r dashboard/requirements.txt
MONGODB_URI=mongodb://localhost:27017 streamlit run dashboard/app.py
```

The Spark processor is easiest to run in Docker (`docker compose up -d processor`).

## 6. Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| `docker compose up` stops with "Set WEATHER_API_KEY…" | `.env` is missing or the key is empty. Copy `.env.example` to `.env` and set the key. |
| Producer logs `403`/`401` or "API key is invalid" | Wrong or inactive WeatherAPI key. Check it at weatherapi.com. |
| Producer exits with "missing or still a placeholder" | `WEATHER_API_KEY` is still `your_weatherapi_key_here`. |
| Dashboard says "Cannot reach MongoDB" | `docker compose ps` – is `mongodb` healthy? Check `MONGODB_URI` if you changed it. |
| Dashboard says "Waiting for weather data" | Producer or processor not running yet. Check `docker compose logs producer processor`. The processor takes a few minutes on first start. |
| Processor is "unhealthy" for the first minutes | Normal while jars download (health start period is 180 s). If it persists, check the logs for network errors resolving `repo1.maven.org`. |
| Processor logs `No module named pymongo` / Python errors | Rebuild: `docker compose build --no-cache processor`. |
| Kafka healthy but nothing arrives | Confirm topic names match in `.env`; run the console-consumer check above. |
| Old Spark checkpoint conflicts after wiping only Kafka | `docker compose down -v` to reset everything together. |
| Port already in use (8501, 9092, 27017, 4040) | Stop the conflicting program or change the left side of the port mapping in `compose.yaml`. |
| Only new data after restart | Default `KAFKA_STARTING_OFFSETS=latest`; set `earliest` and use `down -v` to replay. |
| Timestamps look 8 hours off | Set `DASHBOARD_TIMEZONE` (data is stored in UTC). |

## 7. Screenshot guidance

Save images in `docs/screenshots/` using these names so they are easy to reference:

1. `01-dashboard.png` – Dashboard page with live readings after a few minutes of data
2. `02-historical.png` – Historical Data page with a date range
3. `03-analytics.png` – Analytics page
4. `04-compose-ps.png` – `docker compose ps` showing healthy services
5. `05-mongo-records.png` – the `mongosh` query from section 4

Tips: use a 1440 px-wide browser window, let at least 10–15 minutes of data accumulate before the trend screenshots, and make sure no API key or `.env` contents are visible. Then embed them, e.g. `![Dashboard](docs/screenshots/01-dashboard.png)`.

## 8. Tests

```bash
pip install -r dashboard/requirements.txt -r producer/requirements.txt pyspark==3.5.7 mongomock pytest pyyaml
python -m pytest tests -q
```

The tests cover the producer record contract, the processor's validation and idempotent writes, a local Spark session running the real JSON schema and `foreachBatch` function against valid and broken messages, the dashboard (via Streamlit's AppTest, with an in-memory MongoDB) and consistency of `compose.yaml`/`.env.example` with the code. They do **not** start Docker, Kafka or a real MongoDB; see the end-to-end check in section 4 for that.

## 9. Project layout

```
compose.yaml            .env.example           .gitignore
producer/   producer.py  requirements.txt  Dockerfile
processor/  processor.py                   Dockerfile
dashboard/  app.py       requirements.txt  Dockerfile
tests/      unit and configuration tests
docs/screenshots/       put screenshots here
```
