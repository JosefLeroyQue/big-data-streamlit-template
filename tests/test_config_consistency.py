"""Static checks that compose.yaml, .env.example and the code agree with each other."""
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = yaml.safe_load((ROOT / "compose.yaml").read_text())
SERVICES = COMPOSE["services"]


def resolve(value):
    """Apply ${VAR:-default} defaults the way Compose does when .env is empty."""
    return re.sub(r"\$\{[A-Z_]+(?::[-?])([^}]*)\}", lambda m: m.group(1), str(value))


def env(service):
    return {k: resolve(v) for k, v in SERVICES[service]["environment"].items()}


def source_defaults(path, name):
    text = (ROOT / path).read_text()
    match = re.search(rf'getenv\(\s*"{name}"\s*,\s*"([^"]*)"', text)
    return match.group(1) if match else None


def test_all_services_present():
    assert set(SERVICES) == {"kafka", "kafka-init", "mongodb", "processor", "producer", "dashboard"}


def test_kafka_is_kraft_and_internal_listener_is_service_name():
    k = SERVICES["kafka"]["environment"]
    assert "broker,controller" in k["KAFKA_PROCESS_ROLES"]
    assert "ZOOKEEPER" not in " ".join(k)
    assert "PLAINTEXT://kafka:29092" in k["KAFKA_ADVERTISED_LISTENERS"]
    assert "EXTERNAL://localhost:9092" in k["KAFKA_ADVERTISED_LISTENERS"]


def test_producer_and_processor_use_same_internal_kafka_address_and_topic():
    p, s = env("producer"), env("processor")
    assert p["KAFKA_BOOTSTRAP_SERVERS"] == s["KAFKA_BOOTSTRAP_SERVERS"] == "kafka:29092"
    assert p["KAFKA_TOPIC"] == s["KAFKA_TOPIC"] == "streaming-data"
    assert "kafka:29092" in SERVICES["kafka"]["healthcheck"]["test"][1]


def test_processor_and_dashboard_share_mongo_settings():
    s, d = env("processor"), env("dashboard")
    for key in ("MONGODB_URI", "MONGODB_DATABASE", "MONGODB_COLLECTION"):
        assert s[key] == d[key], key
    assert s["MONGODB_URI"] == "mongodb://mongodb:27017"
    assert (s["MONGODB_DATABASE"], s["MONGODB_COLLECTION"]) == ("weather_db", "weather_data")


def test_no_localhost_between_containers():
    for name in ("processor", "producer", "dashboard"):
        for key, value in env(name).items():
            assert "localhost" not in value and "127.0.0.1" not in value, (name, key)


def test_code_defaults_match_compose():
    for key in ("MONGODB_DATABASE", "MONGODB_COLLECTION"):
        a = source_defaults("processor/processor.py", key)
        b = source_defaults("dashboard/app.py", key)
        assert a == b == env("processor")[key], key
    assert source_defaults("processor/processor.py", "KAFKA_TOPIC") == "streaming-data"
    assert source_defaults("producer/producer.py", "KAFKA_TOPIC") == "streaming-data"
    assert source_defaults("processor/processor.py", "KAFKA_BOOTSTRAP_SERVERS") == "kafka:29092"


def test_env_var_names_used_by_code_are_set_in_compose():
    pairs = {
        "processor/processor.py": "processor",
        "producer/producer.py": "producer",
        "dashboard/app.py": "dashboard",
    }
    for path, service in pairs.items():
        used = set(re.findall(r'getenv\(\s*"([A-Z_]+)"', (ROOT / path).read_text()))
        provided = set(SERVICES[service]["environment"])
        # Optional tuning knobs that intentionally rely on code defaults
        optional = {"DASHBOARD_MAX_DOCS", "KAFKA_CONNECT_RETRIES"}
        assert used - provided - optional == set(), (service, used - provided - optional)


def test_dependencies_use_health_or_completion():
    deps = lambda n: {k: v["condition"] for k, v in SERVICES[n]["depends_on"].items()}
    assert deps("kafka-init") == {"kafka": "service_healthy"}
    assert deps("processor") == {"kafka-init": "service_completed_successfully", "mongodb": "service_healthy"}
    assert deps("producer") == {"kafka-init": "service_completed_successfully"}
    assert deps("dashboard") == {"mongodb": "service_healthy"}
    for name in ("kafka", "mongodb", "processor", "dashboard"):
        assert "healthcheck" in SERVICES[name], name


def test_ports():
    assert "8501:8501" in SERVICES["dashboard"]["ports"]
    assert "9092:9092" in SERVICES["kafka"]["ports"]
    assert "27017:27017" in SERVICES["mongodb"]["ports"]


def test_build_contexts_have_dockerfiles_and_files():
    for name in ("processor", "producer", "dashboard"):
        ctx = ROOT / SERVICES[name]["build"].lstrip("./")
        assert (ctx / "Dockerfile").exists()
    assert (ROOT / "dashboard/requirements.txt").exists() and (ROOT / "producer/requirements.txt").exists()
    assert "COPY processor.py" in (ROOT / "processor/Dockerfile").read_text()


def test_env_example_has_no_real_key_and_covers_compose_vars():
    text = (ROOT / ".env.example").read_text()
    assert "WEATHER_API_KEY=your_weatherapi_key_here" in text
    assert not re.search(r"WEATHER_API_KEY=[0-9a-f]{20,}", text)
    referenced = set(re.findall(r"\$\{([A-Z_]+)", (ROOT / "compose.yaml").read_text()))
    defined = set(re.findall(r"^#?\s*([A-Z_]+)=", text, flags=re.M))
    assert referenced <= defined, referenced - defined


def test_gitignore_covers_secrets_and_caches():
    text = (ROOT / ".gitignore").read_text()
    for pattern in (".env", "__pycache__/", ".venv/", ".pytest_cache/"):
        assert pattern in text
