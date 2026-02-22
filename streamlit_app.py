import streamlit as st
from kiteconnect import KiteConnect
import pandas as pd
from datetime import datetime, timedelta
import time
from pathlib import Path
import sqlite3
import concurrent.futures
import threading

# -------------------------------
# Streamlit Page Config
# -------------------------------
st.set_page_config(page_title="Kite OHLCV Extractor", layout="centered")
st.title("📈 Kite OHLCV Extractor")
st.write(
    "Upload a CSV with column **symbol**, fetch OHLCV data from Kite API, "
    "and auto-save results to CSV periodically to avoid data loss & memory issues."
)

# -------------------------------
# SQLite Token Persistence
# -------------------------------
DB_PATH = "kite_session.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS session (key TEXT PRIMARY KEY, value TEXT)"
    )
    conn.commit()
    conn.close()

def db_get(key: str):
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute("SELECT value FROM session WHERE key=?", (key,)).fetchone()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None

def db_set(key: str, value: str):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "INSERT OR REPLACE INTO session (key, value) VALUES (?, ?)", (key, value)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        st.warning(f"DB write failed: {e}")

def db_delete(key: str):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("DELETE FROM session WHERE key=?", (key,))
        conn.commit()
        conn.close()
    except Exception:
        pass

init_db()

# -------------------------------
# Load Kite API Credentials
# -------------------------------
if "kite" not in st.secrets:
    st.error("Missing Kite API credentials in secrets.toml")
    st.stop()

API_KEY    = st.secrets["kite"]["api_key"]
API_SECRET = st.secrets["kite"]["api_secret"]
REDIRECT_URI = st.secrets["kite"]["redirect_uri"]

kite = KiteConnect(api_key=API_KEY)

# -------------------------------
# Session State ← DB Hydration
# Runs on every page load / refresh
# -------------------------------
if "access_token" not in st.session_state:
    saved_token = db_get("access_token")
    st.session_state["access_token"] = saved_token  # None if not found

# -------------------------------
# Login
# -------------------------------
st.subheader("1️⃣ Login to Kite Connect")

login_url = kite.login_url()

# Only show login link when not already logged in
if not st.session_state["access_token"]:
    st.markdown(f"[🔗 **Click here to login**]({login_url})")

request_token = st.query_params.get("request_token")

if request_token and not st.session_state["access_token"]:
    try:
        data = kite.generate_session(request_token, api_secret=API_SECRET)
        token = data["access_token"]
        st.session_state["access_token"] = token
        db_set("access_token", token)           # ← persist to DB
        st.success("✅ Login successful!")
        st.query_params.clear()
        st.rerun()
    except Exception as e:
        st.error(f"Login failed: {e}")

if not st.session_state["access_token"]:
    st.info("Please login using the link above to continue.")
    st.stop()

# Set authenticated client
kite.set_access_token(st.session_state["access_token"])

# Show success + token (useful for debugging / manual re-use)
access_token_display = st.session_state["access_token"]
st.success("Logged into Kite ✔")
with st.expander("🔑 Session Token (click to reveal)", expanded=False):
    st.code(access_token_display, language=None)
    st.caption(
        "This token is saved locally in `kite_session.db` and restored on page refresh. "
        "It expires at Kite's daily logout time (~3:30 AM IST)."
    )

# Logout button — clears DB + session state
if st.button("🚪 Logout"):
    db_delete("access_token")
    st.session_state["access_token"] = None
    st.query_params.clear()
    st.rerun()

# Validate token with a lightweight API call; auto-clear if expired
try:
    kite.profile()
except Exception:
    st.warning("Session token is invalid or expired. Please login again.")
    db_delete("access_token")
    st.session_state["access_token"] = None
    st.rerun()

# -------------------------------
# Helper: Instruments Cache
# -------------------------------
@st.cache_data(ttl=3600)
def load_instruments():
    df = pd.DataFrame(kite.instruments("NSE"))
    return df

def get_token(symbol: str):
    df = load_instruments()
    row = df[df["tradingsymbol"] == symbol]
    if row.empty:
        return None
    return int(row.iloc[0]["instrument_token"])

# -------------------------------
# Autosave File Management
# -------------------------------
def init_autosave_file():
    if "autosave_path" not in st.session_state:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        st.session_state["autosave_path"] = str(
            Path(f"ohlcv_autosave_{ts}.csv").resolve()
        )
    return st.session_state["autosave_path"]

_write_lock = threading.Lock()

def flush_buffer_to_csv(buffer, autosave_path, file_written_flag):
    if not buffer:
        return file_written_flag
    df_buffer = pd.DataFrame(buffer)
    with _write_lock:
        df_buffer.to_csv(
            autosave_path,
            mode="a",
            header=not file_written_flag,
            index=False,
        )
    return True

# -------------------------------
# Worker: Fetch one symbol
# -------------------------------
def fetch_symbol(args):
    """Runs in a thread pool. Returns list of row-dicts."""
    sym, token_id, from_dt, to_dt = args
    rows = []

    if token_id is None:
        rows.append(
            dict(symbol=sym, date=None, open=None, high=None,
                 low=None, close=None, volume=None, error="Token not found")
        )
        return rows

    try:
        data = kite.historical_data(
            token_id,
            from_date=from_dt,
            to_date=to_dt,
            interval="day",
        )
        for r in data:
            rows.append(
                dict(symbol=sym, date=r.get("date"), open=r.get("open"),
                     high=r.get("high"), low=r.get("low"), close=r.get("close"),
                     volume=r.get("volume"), error=None)
            )
    except Exception as e:
        rows.append(
            dict(symbol=sym, date=None, open=None, high=None,
                 low=None, close=None, volume=None, error=str(e))
        )
    return rows

# -------------------------------
# CSV Upload
# -------------------------------
st.subheader("2️⃣ Upload CSV with column **symbol**")

uploaded = st.file_uploader("Upload CSV", type=["csv"], key="symbols_uploader")

if uploaded is not None:
    df_symbols = pd.read_csv(uploaded)
    st.session_state["df_symbols"] = df_symbols
elif "df_symbols" in st.session_state:
    df_symbols = st.session_state["df_symbols"]
else:
    df_symbols = None

if df_symbols is not None:
    if "symbol" not in df_symbols.columns:
        st.error("CSV must contain column: **symbol**")
        st.stop()

    st.write("Uploaded Symbols:")
    st.dataframe(df_symbols)

    # -------------------------------
    # Fetch OHLCV
    # -------------------------------
    st.subheader("3️⃣ Fetch OHLCV Data")

    from_date = st.date_input(
        "From Date",
        datetime.now().date() - timedelta(days=30),
        key="from_date",
    )
    to_date = st.date_input("To Date", datetime.now().date(), key="to_date")

    max_workers = st.slider(
        "Parallel workers (threads)",
        min_value=1, max_value=10, value=5,
        help="Higher = faster but may hit Kite rate limits."
    )

    flush_interval = st.number_input(
        "Autosave every N seconds", min_value=10, max_value=120,
        value=30, step=10
    )

    st.caption(
        "Results are streamed to a CSV every ~N seconds. "
        "Data is safe even if the browser tab closes mid-run."
    )

    if st.button("🚀 Fetch Data", key="fetch_data_btn"):
        autosave_path = init_autosave_file()
        autosave_file = Path(autosave_path)
        if autosave_file.exists():
            try:
                autosave_file.unlink()
            except Exception as e:
                st.warning(f"Could not remove old autosave file: {e}")

        st.info(f"Autosaving to: `{autosave_path}`")

        progress        = st.progress(0)
        status_ph       = st.empty()
        autosave_ph     = st.empty()

        symbols = [str(s).strip().upper() for s in df_symbols["symbol"]]
        total   = len(symbols)

        # Pre-fetch all tokens (cached, fast)
        status_ph.write("🔍 Resolving instrument tokens…")
        token_map = {sym: get_token(sym) for sym in symbols}

        from_dt = datetime.combine(from_date, datetime.min.time())
        to_dt   = datetime.combine(to_date, datetime.max.time())

        args_list = [
            (sym, token_map[sym], from_dt, to_dt)
            for sym in symbols
        ]

        rows_buffer   = []
        file_written  = False
        last_flush    = time.time()
        completed     = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_symbol, args): args[0] for args in args_list}

            for future in concurrent.futures.as_completed(futures):
                sym = futures[future]
                completed += 1
                status_ph.write(f"✅ Done: **{sym}** ({completed}/{total})")

                try:
                    rows = future.result()
                    rows_buffer.extend(rows)
                except Exception as e:
                    rows_buffer.append(
                        dict(symbol=sym, date=None, open=None, high=None,
                             low=None, close=None, volume=None, error=str(e))
                    )

                now = time.time()
                should_flush = (
                    now - last_flush >= flush_interval
                    or len(rows_buffer) >= 5000
                    or completed == total
                )

                if should_flush and rows_buffer:
                    file_written = flush_buffer_to_csv(
                        rows_buffer, autosave_path, file_written
                    )
                    last_flush = now
                    autosave_ph.success(
                        f"💾 Autosaved {len(rows_buffer)} rows at "
                        f"{datetime.now().strftime('%H:%M:%S')}"
                    )
                    rows_buffer = []

                progress.progress(completed / total)

        st.success("✅ Data fetching completed!")

        if autosave_file.exists():
            try:
                df_out = pd.read_csv(autosave_file)
                st.write(f"Preview (first 500 of {len(df_out):,} rows):")
                st.dataframe(df_out.head(500))

                with open(autosave_file, "rb") as f:
                    st.download_button(
                        label="📥 Download Complete OHLCV CSV",
                        data=f.read(),
                        file_name="ohlcv_output.csv",
                        mime="text/csv",
                    )
            except Exception as e:
                st.error(
                    f"File saved at `{autosave_path}` but preview failed: {e}. "
                    "You can download it directly from the server."
                )
        else:
            st.error(
                "Unexpected: autosave file not found after run. "
                "Check disk permissions."
            )

else:
    st.info("Upload a CSV with a **symbol** column to begin.")
