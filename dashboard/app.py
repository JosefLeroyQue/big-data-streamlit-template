import math
import os
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

try:
    from pymongo import MongoClient
except ImportError:
    MongoClient = None

try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:
    st_autorefresh = None

# -----------------------------
# Page setup
# -----------------------------
st.set_page_config(
    page_title="Weather Weather Lang",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -----------------------------
# Palette (single source of truth for CSS and charts)
# -----------------------------
BG = "#071a33"
SIDEBAR = "#051427"
CARD = "#0d2748"
BORDER = "#1f4272"
TEXT = "#f8fbff"
MUTED = "#9db3d4"
YELLOW = "#ffd447"
CYAN = "#7fc3ff"
GRID = "rgba(157, 179, 212, 0.14)"
FONT = "-apple-system, 'Segoe UI', Roboto, Arial, sans-serif"

CSS = """
html, body, .stApp { font-family: var(--font); }
.stApp { background: var(--bg); color: var(--text); }
[data-testid="stHeader"] { background: var(--bg); }
.block-container { padding-top: 2rem; padding-bottom: 2rem; max-width: 1500px; }

/* Text colors are set here so the app does not depend on Streamlit's theme */
.stApp h1, .stApp h2, .stApp h3, .stApp h4 { color: var(--text) !important; font-weight: 600; letter-spacing: 0; }
[data-testid="stMarkdownContainer"] p, [data-testid="stMarkdownContainer"] li { color: var(--text); }
[data-testid="stWidgetLabel"], [data-testid="stWidgetLabel"] * { color: var(--text) !important; }
[data-testid="stCaptionContainer"], [data-testid="stCaptionContainer"] * { color: var(--muted) !important; }
[data-testid="stHeader"] * { color: var(--muted) !important; }
[data-testid="stRadio"] label, [data-testid="stRadio"] label * { color: var(--text) !important; }

/* Sidebar */
[data-testid="stSidebar"] { background: var(--sidebar); border-right: 1px solid var(--border); }
.brand { font-size: 1.05rem; font-weight: 700; color: var(--text); }
.brand-sub { font-size: .8rem; color: var(--muted); margin-top: 2px; }

/* Cards: every bordered container shares one look */
[class*="st-key-card-"] {
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 8px;
  box-shadow: none;
}
.card-title { font-size: 1rem; font-weight: 600; color: var(--text); line-height: 1.3; }
.card-sub { font-size: .8rem; color: var(--muted); margin: 2px 0 .75rem 0; }

/* Metrics use the same card surface */
div[data-testid="stMetric"] {
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 1rem 1.25rem;
  min-height: 7rem;
  box-shadow: none;
}
div[data-testid="stMetricLabel"] { color: var(--muted); font-size: .8rem; }
div[data-testid="stMetricValue"] { color: var(--text); font-weight: 600; }
div[data-testid="stMetricDelta"] { color: var(--muted); font-size: .8rem; background: transparent !important; padding-left: 0; }

/* Hero */
.hero-location { font-size: 1.1rem; font-weight: 600; color: var(--text); }
.hero-temp { font-size: 3.5rem; font-weight: 700; line-height: 1.1; color: var(--yellow); margin: 1.25rem 0 .5rem 0; }
.hero-note { color: var(--muted); font-size: .9rem; }

/* Comfort */
.comfort-score { font-size: 3.5rem; font-weight: 700; line-height: 1.1; color: var(--yellow); margin-top: 1rem; }
.comfort-score span { font-size: 1.5rem; margin-left: 2px; }
.comfort-level { font-size: .95rem; color: var(--text); margin: .25rem 0 1.25rem 0; }
.comfort-track { position: relative; height: 10px; background: var(--bg); border: 1px solid var(--border); border-radius: 5px; }
.comfort-fill { height: 100%; background: var(--yellow); border-radius: 4px; }
.comfort-track i { position: absolute; top: -3px; bottom: -3px; width: 1px; background: var(--muted); opacity: .6; }
.comfort-scale { position: relative; height: 1.2rem; margin-top: .4rem; font-size: .72rem; color: var(--muted); }
.comfort-scale span { position: absolute; transform: translateX(-50%); }
.comfort-scale span:first-child { transform: none; }
.comfort-scale span:last-child { transform: translateX(-100%); }
.comfort-note { font-size: .8rem; color: var(--muted); margin-top: .75rem; }

/* Key/value rows */
.stat-row {
  display: flex; justify-content: space-between; align-items: center;
  padding: .6rem 0; border-bottom: 1px solid var(--border);
}
.stat-row:last-child { border-bottom: 0; }
.stat-row .label { color: var(--muted); font-size: .85rem; }
.stat-row .value { color: var(--text); font-weight: 600; font-size: .95rem; }

/* Pipeline flow */
.flow { display: flex; flex-wrap: wrap; align-items: center; gap: .6rem; margin: 1rem 0; }
.flow .step {
  border: 1px solid var(--border); background: var(--bg); border-radius: 6px;
  padding: .5rem .8rem; font-size: .88rem; color: var(--text);
}
.flow .arrow { color: var(--muted); }

.header-meta { text-align: right; padding-top: .35rem; color: var(--muted); font-size: .85rem; line-height: 1.6; }
.header-meta strong { color: var(--text); font-weight: 600; }

/* Buttons: one style */
.stButton > button, .stDownloadButton > button {
  background: var(--card); color: var(--text);
  border: 1px solid var(--border); border-radius: 6px; font-weight: 500; box-shadow: none;
}
.stButton > button *, .stDownloadButton > button * { color: inherit !important; }
.stButton > button:hover, .stDownloadButton > button:hover {
  border-color: var(--yellow); color: var(--yellow); background: var(--card);
}

/* Select boxes and date inputs (new react-aria markup first, older baseweb markup after) */
[data-testid="stSelectbox"] [role="group"], [data-testid="stDateInputField"],
[data-baseweb="select"] > div, [data-baseweb="input"], [data-baseweb="base-input"] {
  background: var(--bg) !important; border: 1px solid var(--border) !important; border-radius: 6px !important;
}
[data-testid="stSelectbox"] input, [data-testid="stDateInputField"] *,
[data-baseweb="select"] *, [data-baseweb="input"] input {
  color: var(--text) !important; -webkit-text-fill-color: var(--text) !important; background-color: transparent;
}
[data-testid="stSelectbox"] button, [data-testid="stDateInput"] button { color: var(--muted) !important; background: transparent !important; }
[data-testid="stSelectbox"] svg, [data-testid="stDateInput"] svg { fill: var(--muted) !important; }

/* Dropdown list and calendar popups */
[data-testid="stSelectboxVirtualDropdown"], [data-testid="stDateInputCalendar"],
[data-baseweb="popover"] > div, [data-baseweb="menu"], [data-baseweb="calendar"] {
  background: var(--card) !important; border: 1px solid var(--border) !important; border-radius: 6px !important;
}
[data-testid="stSelectboxVirtualDropdown"] *, [data-testid="stDateInputCalendar"] *, [data-baseweb="popover"] li {
  color: var(--text) !important; -webkit-text-fill-color: var(--text) !important;
}
[data-testid="stSelectboxVirtualDropdown"] [role="option"][aria-selected="true"],
[data-testid="stSelectboxVirtualDropdown"] [role="option"]:hover,
[data-testid="stDateInputCalendar"] [role="gridcell"] button:hover,
[data-baseweb="popover"] li:hover { background: var(--bg) !important; }
[data-testid="stDateInputCalendar"] [aria-selected="true"], [data-testid="stDateInputCalendar"] [data-selected="true"] {
  background: var(--yellow) !important; border-radius: 6px;
}
[data-testid="stDateInputCalendar"] [aria-selected="true"] *, [data-testid="stDateInputCalendar"] [data-selected="true"] * {
  color: var(--sidebar) !important; -webkit-text-fill-color: var(--sidebar) !important;
}

/* Radio: dark ring, yellow when selected */
[data-testid="stRadioOption"] > div > div:first-child, [data-baseweb="radio"] > div:first-child {
  background: var(--bg) !important; border: 1px solid var(--border) !important; box-shadow: none !important;
}
[data-testid="stRadioOption"] > div > div:first-child > div { background: var(--bg) !important; }
[data-testid="stRadioOption"][data-selected="true"] > div > div:first-child {
  background: var(--yellow) !important; border-color: var(--yellow) !important;
}
[data-baseweb="radio"] input:checked + div { background: var(--yellow) !important; border-color: var(--yellow) !important; }

/* Tables rendered as HTML so they match the cards */
.table-wrap { border: 1px solid var(--border); border-radius: 8px; overflow: auto; background: var(--bg); margin-bottom: .75rem; }
.data-table { width: 100%; border-collapse: collapse; font-size: .85rem; color: var(--text); }
.data-table th {
  position: sticky; top: 0; background: var(--card); color: var(--muted); font-weight: 600;
  text-align: left; padding: .55rem .75rem; border-bottom: 1px solid var(--border); white-space: nowrap;
}
.data-table td { padding: .5rem .75rem; border-bottom: 1px solid var(--border); white-space: nowrap; color: var(--text); }
.data-table tr:last-child td { border-bottom: 0; }

.codeblock {
  background: var(--bg); border: 1px solid var(--border); border-radius: 6px;
  padding: .8rem 1rem; margin: 0; color: var(--text); font-size: .85rem; overflow-x: auto;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
}
hr { border-color: var(--border) !important; }
"""

ROOT = (
    f":root {{ color-scheme: dark; --bg:{BG}; --sidebar:{SIDEBAR}; --card:{CARD}; --border:{BORDER}; "
    f"--text:{TEXT}; --muted:{MUTED}; --yellow:{YELLOW}; --cyan:{CYAN}; --font:{FONT}; }}"
)
st.markdown(f"<style>{ROOT}{CSS}</style>", unsafe_allow_html=True)


# -----------------------------
# UI helpers
# -----------------------------
def card(key, height=None):
    """Bordered container. All cards on every page go through here."""
    options = {"height": height} if height else {}
    return st.container(border=True, key=f"card-{key}", **options)


def card_header(title, subtitle=""):
    sub = f'<div class="card-sub">{subtitle}</div>' if subtitle else '<div class="card-sub">&nbsp;</div>'
    st.markdown(f'<div class="card-title">{title}</div>{sub}', unsafe_allow_html=True)


def html_table(data, index=False, max_height=None):
    """Static table drawn as HTML so it follows the palette instead of Streamlit's theme."""
    table = data.to_html(index=index, border=0, classes="data-table", justify="left", na_rep="")
    style = f' style="max-height:{max_height}px"' if max_height else ""
    st.markdown(f'<div class="table-wrap"{style}>{table.replace(chr(10), "")}</div>', unsafe_allow_html=True)


def style_fig(fig, height):
    fig.update_layout(
        height=height,
        margin=dict(l=0, r=0, t=8, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=FONT, color=MUTED, size=12),
        legend=dict(orientation="h", y=1.14, x=0, bgcolor="rgba(0,0,0,0)", font=dict(color=TEXT)),
        hovermode="x unified",
        hoverlabel=dict(bgcolor=BG, bordercolor=BORDER, font=dict(color=TEXT)),
    )
    fig.update_xaxes(showgrid=False, zeroline=False, linecolor=BORDER, tickfont=dict(color=MUTED), title_font=dict(color=MUTED))
    fig.update_yaxes(gridcolor=GRID, zeroline=False, tickfont=dict(color=MUTED), title_font=dict(color=MUTED))
    return fig


def temp_humidity_chart(data, height):
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=data["timestamp"], y=data["temperature"], name="Temperature (°C)",
        mode="lines", line=dict(color=YELLOW, width=2.5),
    ))
    fig.add_trace(go.Scatter(
        x=data["timestamp"], y=data["humidity"], name="Humidity (%)",
        mode="lines", line=dict(color=CYAN, width=2.5), yaxis="y2",
    ))
    style_fig(fig, height)
    fig.update_layout(
        yaxis=dict(title="°C"),
        yaxis2=dict(
            title="%", overlaying="y", side="right", showgrid=False, zeroline=False,
            tickfont=dict(color=MUTED), title_font=dict(color=MUTED),
        ),
    )
    return fig


PLOT_CONFIG = {"displayModeBar": False}


# -----------------------------
# Configuration (same variable names as the Spark processor)
# -----------------------------
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "weather_db")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "weather_data")
MAX_DOCS = int(os.getenv("DASHBOARD_MAX_DOCS", "6000"))
REFRESH_SECONDS = int(os.getenv("DASHBOARD_REFRESH_SECONDS", "20"))
DEMO_MODE = os.getenv("DASHBOARD_DEMO_MODE", "false").strip().lower() in ("1", "true", "yes")

try:
    ZoneInfo(os.getenv("DASHBOARD_TIMEZONE", "Asia/Manila"))
    DISPLAY_TZ = os.getenv("DASHBOARD_TIMEZONE", "Asia/Manila")
except Exception:
    DISPLAY_TZ = "UTC"

VALUE_COLUMNS = ["temperature", "humidity", "wind_speed", "rainfall"]
CORE_COLUMNS = ["temperature", "humidity", "wind_speed"]

# metric_type values written by the producer/processor -> dashboard column
METRIC_ALIASES = {
    "temperature": "temperature", "temp": "temperature", "temp_c": "temperature",
    "humidity": "humidity",
    "wind_speed": "wind_speed", "wind_kph": "wind_speed", "wind": "wind_speed",
    "rainfall": "rainfall", "precip_mm": "rainfall", "precipitation": "rainfall",
}

# Column names of older wide-format documents
WIDE_ALIASES = {
    "temp": "temperature", "temp_c": "temperature", "temperature_c": "temperature",
    "humidity_pct": "humidity", "wind_kph": "wind_speed", "wind": "wind_speed",
    "precip_mm": "rainfall", "rain_mm": "rainfall", "condition_text": "condition",
    "city": "location", "name": "location", "time": "timestamp", "datetime": "timestamp",
}


# -----------------------------
# Data source: MongoDB (long format -> wide format)
# -----------------------------
def parse_timestamps(series):
    """Parse mixed datetimes/ISO strings as UTC, then show them in DISPLAY_TZ (tz-naive)."""
    parsed = pd.to_datetime(series, errors="coerce", utc=True, format="mixed")
    return parsed.dt.tz_convert(DISPLAY_TZ).dt.tz_localize(None)


def mask_uri(uri):
    return re.sub(r"//[^/@]*@", "//***@", uri)


def fetch_documents():
    if MongoClient is None:
        raise RuntimeError("pymongo is not installed")
    client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=2500, tz_aware=True)
    try:
        client.admin.command("ping")
        return list(
            client[MONGODB_DATABASE][MONGODB_COLLECTION]
            .find({}, {"_id": 0})
            .sort([("timestamp", -1)])
            .limit(MAX_DOCS)
        )
    finally:
        client.close()


def finalize_weather_data(df):
    """Make sure every dashboard column exists and drop rows that cannot be plotted."""
    for column in VALUE_COLUMNS:
        if column not in df:
            df[column] = np.nan
        df[column] = pd.to_numeric(df[column], errors="coerce")
    if "location" not in df:
        df["location"] = "Unknown"
    if "condition" not in df:
        df["condition"] = "Weather data"
    df["location"] = df["location"].fillna("Unknown").astype(str)
    df["condition"] = df["condition"].fillna("Weather data").astype(str)
    df = df.dropna(subset=["timestamp"] + CORE_COLUMNS)
    return df.sort_values("timestamp").reset_index(drop=True)


def long_to_wide(raw):
    """Pivot processor records (metric_type + value) into one row per location/timestamp."""
    raw = raw.copy()
    raw["metric"] = raw["metric_type"].astype(str).str.strip().str.lower().map(METRIC_ALIASES)
    raw["value"] = pd.to_numeric(raw["value"], errors="coerce")
    raw["timestamp"] = parse_timestamps(raw["timestamp"]).dt.floor("s")
    for column, default in (("location", "Unknown"), ("condition", "Weather data")):
        if column not in raw:
            raw[column] = default
        raw[column] = raw[column].fillna(default)
    raw = raw.dropna(subset=["metric", "value", "timestamp"]).sort_values("timestamp")
    if raw.empty:
        return raw

    wide = raw.pivot_table(
        index=["location", "timestamp"], columns="metric", values="value", aggfunc="last"
    )
    meta = raw.groupby(["location", "timestamp"])[["condition"]].last()
    return wide.join(meta).reset_index()


def normalize_weather_data(raw):
    """Accept long-format (processor) or legacy wide-format documents."""
    raw = raw.rename(columns={c: str(c) for c in raw.columns})
    if "metric_type" in raw.columns and "value" in raw.columns:
        df = long_to_wide(raw)
        if df.empty:
            return df
    else:
        df = raw.rename(columns={c: WIDE_ALIASES[c.lower()] for c in raw.columns if c.lower() in WIDE_ALIASES})
        if "timestamp" not in df:
            return df.iloc[0:0]
        df["timestamp"] = parse_timestamps(df["timestamp"])
    return finalize_weather_data(df)


def demo_data():
    """Only used when DASHBOARD_DEMO_MODE=true, so a fake chart is never shown by accident."""
    now = datetime.now().replace(minute=0, second=0, microsecond=0)
    rows = []
    for i in range(48):
        ts = now - timedelta(hours=47 - i)
        rows.append({
            "timestamp": ts,
            "location": "Manila",
            "temperature": round(27 + 3.2 * math.sin(i / 5.5) + 0.55 * math.sin(i / 2.1), 1),
            "humidity": round(max(48, min(95, 74 - 9 * math.sin(i / 7.0) + 3 * math.cos(i / 3.5))), 1),
            "wind_speed": round(8 + 4 * abs(math.sin(i / 6)), 1),
            "rainfall": round(max(0, 1.8 * math.sin(i / 4.2) - 0.8), 1),
            "condition": "Partly cloudy" if i % 3 else "Light rain",
        })
    return pd.DataFrame(rows)


@st.cache_data(ttl=15, show_spinner=False)
def load_weather_data():
    """Returns (dataframe, source label, status, message). status: ok | empty | error | demo."""
    empty = pd.DataFrame(columns=["timestamp", "location", "condition"] + VALUE_COLUMNS)
    if DEMO_MODE:
        return demo_data(), "Demo data", "demo", ""
    try:
        docs = fetch_documents()
    except Exception as error:
        return empty, "MongoDB", "error", (
            f"Could not connect to MongoDB at {mask_uri(MONGODB_URI)} "
            f"({type(error).__name__}). Check that the mongodb service is running "
            f"and that MONGODB_URI is correct."
        )
    if not docs:
        return empty, "MongoDB", "empty", (
            f"Connected to MongoDB, but {MONGODB_DATABASE}.{MONGODB_COLLECTION} has no records yet. "
            f"Start the producer and the Spark processor; the first readings appear within a minute."
        )
    df = normalize_weather_data(pd.DataFrame(docs))
    if df.empty:
        return df, "MongoDB", "empty", (
            f"{MONGODB_DATABASE}.{MONGODB_COLLECTION} has {len(docs)} documents, but none contain "
            f"complete temperature, humidity and wind-speed readings yet."
        )
    return df, "MongoDB", "ok", ""


def fmt(value, spec=".1f", unit=""):
    """Format a number for display; NaN becomes n/a."""
    if value is None or pd.isna(value):
        return "n/a"
    return f"{value:{spec}}{unit}"


df, data_source, data_status, data_message = load_weather_data()

# -----------------------------
# Sidebar
# -----------------------------
with st.sidebar:
    st.markdown(
        '<div class="brand">Weather Weather Lang</div><div class="brand-sub">Team 3 weather dashboard</div>',
        unsafe_allow_html=True,
    )
    st.divider()
    page = st.radio(
        "Navigation",
        ["Dashboard", "Historical Data", "Analytics", "About Pipeline"],
        label_visibility="collapsed",
    )
    st.divider()
    selected_location = None
    if not df.empty:
        locations = sorted(df["location"].dropna().astype(str).unique().tolist())
        selected_location = st.selectbox("Location", locations, index=0)
    st.caption(f"Data source: {data_source} · times in {DISPLAY_TZ}")
    auto_refresh = st.checkbox(f"Auto-refresh every {REFRESH_SECONDS}s", value=True)
    if st.button("Refresh data", width="stretch"):
        st.cache_data.clear()
        st.rerun()

if auto_refresh and st_autorefresh is not None:
    st_autorefresh(interval=REFRESH_SECONDS * 1000, key="weather_autorefresh")

if df.empty:
    st.markdown("## Weather Weather Lang")
    with card("nodata"):
        card_header(
            "Cannot reach MongoDB" if data_status == "error" else "Waiting for weather data",
            "The dashboard will update automatically once data is available",
        )
        st.markdown(data_message)
    st.stop()

view = df[df["location"].astype(str) == str(selected_location)].copy()
if view.empty:
    view = df.copy()
view = view.sort_values("timestamp")
latest = view.iloc[-1]
previous = view.iloc[-2] if len(view) > 1 else latest
temp_delta = float(latest["temperature"] - previous["temperature"])
humidity_delta = float(latest["humidity"] - previous["humidity"])
wind_delta = float(latest["wind_speed"] - previous["wind_speed"])
condition = str(latest.get("condition", "Weather data"))
updated = pd.Timestamp(latest["timestamp"]).strftime("%b %d, %H:%M")

# -----------------------------
# Page header
# -----------------------------
head_l, head_r = st.columns([3, 1])
with head_l:
    st.markdown(f"## {page}")
    st.caption(str(selected_location))
with head_r:
    st.markdown(
        f'<div class="header-meta">Source: <strong>{data_source}</strong><br>Updated {updated}</div>',
        unsafe_allow_html=True,
    )

# -----------------------------
# Dashboard
# -----------------------------
if page == "Dashboard":
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Temperature", f'{latest["temperature"]:.1f} °C', f"{temp_delta:+.1f} °C", delta_color="off")
    k2.metric("Humidity", f'{latest["humidity"]:.0f}%', f"{humidity_delta:+.0f}%", delta_color="off")
    k3.metric("Wind speed", f'{latest["wind_speed"]:.1f} km/h', f"{wind_delta:+.1f} km/h", delta_color="off")
    k4.metric("Rainfall, current", fmt(latest["rainfall"], ".1f", " mm"))

    left, middle, right = st.columns([1.4, 1, 1.2], gap="medium")
    with left:
        with card("hero", height=340):
            card_header("Current conditions", "Latest reading for the selected location")
            st.markdown(
                f"""
                <div class="hero-location">{selected_location}</div>
                <div class="hero-temp">{latest['temperature']:.1f} °C</div>
                <div class="hero-note">{condition}</div>
                <div class="hero-note">Recorded {updated}</div>
                """,
                unsafe_allow_html=True,
            )
    with middle:
        with card("comfort", height=340):
            card_header("Weather comfort", "Based on temperature and humidity")
            comfort = max(0, min(100, 100 - abs(float(latest["temperature"]) - 24) * 3 - abs(float(latest["humidity"]) - 55) * 0.45))
            level = "Comfortable" if comfort >= 70 else "Moderate" if comfort >= 45 else "Needs attention"
            st.markdown(
                f"""
                <div class="comfort-score">{comfort:.0f}<span>%</span></div>
                <div class="comfort-level">{level}</div>
                <div class="comfort-track">
                  <div class="comfort-fill" style="width:{comfort:.0f}%"></div>
                  <i style="left:45%"></i><i style="left:70%"></i>
                </div>
                <div class="comfort-scale">
                  <span style="left:0">0</span><span style="left:45%">45</span>
                  <span style="left:70%">70</span><span style="left:100%">100</span>
                </div>
                <div class="comfort-note">Scores drop as temperature moves away from 24 °C and humidity away from 55%.</div>
                """,
                unsafe_allow_html=True,
            )
    with right:
        with card("live", height=340):
            card_header("Live conditions", "Latest readings")
            rows = [
                ("Temperature", f'{latest["temperature"]:.1f} °C'),
                ("Relative humidity", f'{latest["humidity"]:.0f}%'),
                ("Wind speed", f'{latest["wind_speed"]:.1f} km/h'),
                ("Rainfall", fmt(latest["rainfall"], ".1f", " mm")),
                ("Latest record", pd.Timestamp(latest["timestamp"]).strftime("%H:%M")),
            ]
            st.markdown(
                "".join(f'<div class="stat-row"><span class="label">{k}</span><span class="value">{v}</span></div>' for k, v in rows),
                unsafe_allow_html=True,
            )

    chart_col, rain_col = st.columns([1.6, 1], gap="medium")
    with chart_col:
        with card("trends"):
            card_header("Weather trends", "Temperature and humidity, last 48 records")
            st.plotly_chart(temp_humidity_chart(view.tail(48), 280), width="stretch", config=PLOT_CONFIG)
    with rain_col:
        with card("rain"):
            card_header("Rainfall activity", "Precipitation, last 12 records")
            rain_data = view.tail(12).copy()
            rain_data["rainfall"] = rain_data["rainfall"].fillna(0)
            bar = go.Figure(go.Bar(
                x=rain_data["timestamp"], y=rain_data["rainfall"],
                marker=dict(color=CYAN, line=dict(width=0)), name="Rainfall (mm)",
            ))
            style_fig(bar, 280)
            bar.update_layout(showlegend=False, yaxis=dict(title="mm"), xaxis=dict(tickformat="%H:%M"))
            st.plotly_chart(bar, width="stretch", config=PLOT_CONFIG)

    with card("recent"):
        card_header("Recent weather records", "Most recent observations in the stream")
        recent = view.sort_values("timestamp", ascending=False).head(8).copy()
        recent["timestamp"] = recent["timestamp"].dt.strftime("%b %d, %Y %H:%M")
        recent = recent[["timestamp", "location", "condition", "temperature", "humidity", "wind_speed", "rainfall"]]
        recent.columns = ["Timestamp", "Location", "Condition", "Temp (°C)", "Humidity (%)", "Wind (km/h)", "Rain (mm)"]
        html_table(recent)

# -----------------------------
# Historical data
# -----------------------------
elif page == "Historical Data":
    with card("history"):
        card_header("Historical weather records", "Filter and inspect the records in your weather collection")
        min_date = view["timestamp"].min().date()
        max_date = view["timestamp"].max().date()
        d1, d2 = st.columns(2)
        with d1:
            start_date = st.date_input("From", value=min_date, min_value=min_date, max_value=max_date)
        with d2:
            end_date = st.date_input("To", value=max_date, min_value=min_date, max_value=max_date)
        if start_date > end_date:
            st.warning("The start date is after the end date.")
        filtered = view[(view["timestamp"].dt.date >= start_date) & (view["timestamp"].dt.date <= end_date)]
        st.caption(f"{len(filtered):,} records")
        display = filtered.sort_values("timestamp", ascending=False).copy()
        display["timestamp"] = display["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
        html_table(display, max_height=420)
        st.download_button("Download CSV", filtered.to_csv(index=False).encode("utf-8"), "weather_history.csv", "text/csv")

# -----------------------------
# Analytics
# -----------------------------
elif page == "Analytics":
    a, b = st.columns(2, gap="medium")
    with a:
        with card("distribution"):
            card_header("Temperature distribution", "Number of records per temperature range")
            hist = px.histogram(view, x="temperature", nbins=12, color_discrete_sequence=[YELLOW])
            hist.update_traces(marker_line_width=1, marker_line_color=CARD)
            style_fig(hist, 320)
            hist.update_layout(showlegend=False, xaxis_title="Temperature (°C)", yaxis_title="Records", bargap=0.05)
            st.plotly_chart(hist, width="stretch", config=PLOT_CONFIG)
    with b:
        with card("daily"):
            card_header("Daily average readings", "Temperature and humidity")
            daily = view.set_index("timestamp").resample("D")[["temperature", "humidity"]].mean().reset_index()
            st.plotly_chart(temp_humidity_chart(daily, 320), width="stretch", config=PLOT_CONFIG)

    with card("stats"):
        card_header("Summary statistics", "Across all records for the selected location")
        stats = view[["temperature", "humidity", "wind_speed", "rainfall"]].describe().round(2)
        html_table(stats, index=True)

# -----------------------------
# About pipeline
# -----------------------------
else:
    with card("flow"):
        card_header("Weather data flow", "Project architecture")
        steps = ["WeatherAPI", "Kafka producer", "Kafka topic", "Spark Structured Streaming", "MongoDB", "Streamlit"]
        flow = '<span class="arrow">&rarr;</span>'.join(f'<span class="step">{s}</span>' for s in steps)
        st.markdown(f'<div class="flow">{flow}</div>', unsafe_allow_html=True)
        st.caption(
            "The producer publishes one record per metric to Kafka. Spark validates the records and upserts them "
            "into MongoDB, and this dashboard pivots them into temperature, humidity, wind speed and rainfall."
        )

    with card("config"):
        card_header("MongoDB configuration", "Active settings (environment variables read at startup)")
        st.markdown(
            '<pre class="codeblock">'
            f"MONGODB_URI={mask_uri(MONGODB_URI)}\n"
            f"MONGODB_DATABASE={MONGODB_DATABASE}\n"
            f"MONGODB_COLLECTION={MONGODB_COLLECTION}\n"
            f"DASHBOARD_TIMEZONE={DISPLAY_TZ}"
            "</pre>",
            unsafe_allow_html=True,
        )