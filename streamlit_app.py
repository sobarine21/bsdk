import streamlit as st
from kiteconnect import KiteConnect
import pandas as pd
from datetime import datetime, timedelta
import time
from pathlib import Path
import os

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
# Load Kite API Credentials
# -------------------------------
if "kite" not in st.secrets:
    st.error("Missing Kite API credentials in secrets.toml")
    st.stop()

API_KEY = st.secrets["kite"]["api_key"]
API_SECRET = st.secrets["kite"]["api_secret"]
REDIRECT_URI = st.secrets["kite"]["redirect_uri"]

kite = KiteConnect(api_key=API_KEY)

# -------------------------------
# Login
# -------------------------------
st.subheader("1️⃣ Login to Kite Connect")

login_url = kite.login_url()
st.markdown(f"[🔗 **Click here to login**]({login_url})")

request_token = st.query_params.get("request_token")

if "access_token" not in st.session_state:
    st.session_state["access_token"] = None

if request_token and not st.session_state["access_token"]:
    try:
        data = kite.generate_session(request_token, api_secret=API_SECRET)
        st.session_state["access_token"] = data["access_token"]
        st.success("✅ Login successful!")
        st.query_params.clear()
        st.rerun()
    except Exception as e:
        st.error(f"Login failed: {e}")

if not st.session_state["access_token"]:
    st.stop()

# Setup authenticated client
kite.set_access_token(st.session_state["access_token"])
st.success("Logged into Kite ✔")

# -------------------------------
# Helper to get instrument token
# -------------------------------
@st.cache_data(ttl=3600)
def load_instruments():
    """Load NSE instruments once and cache."""
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
    """Initialize a unique autosave CSV path for this run."""
    if "autosave_path" not in st.session_state:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        autosave_name = f"ohlcv_autosave_{timestamp}.csv"
        st.session_state["autosave_path"] = str(Path(autosave_name).resolve())
    else:
        # Keep existing file path; don't recreate on every rerun
        pass
    return st.session_state["autosave_path"]


def flush_buffer_to_csv(buffer, autosave_path, file_written_flag):
    """Append buffered rows to CSV on disk (streaming style)."""
    if not buffer:
        return file_written_flag

    df_buffer = pd.DataFrame(buffer)

    # Append to file; write header only if file is new
    df_buffer.to_csv(
        autosave_path,
        mode="a",
        header=not file_written_flag,
        index=False,
    )
    return True  # File has now been written at least once


# -------------------------------
# CSV Upload
# -------------------------------
st.subheader("2️⃣ Upload CSV with column **symbol**")

uploaded = st.file_uploader("Upload CSV", type=["csv"], key="symbols_uploader")

# Persist symbols in session_state to survive reruns
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
        "From Date", datetime.now().date() - timedelta(days=30), key="from_date"
    )
    to_date = st.date_input("To Date", datetime.now().date(), key="to_date")

    st.caption(
        "When you click **Fetch Data**, the app will stream results to a CSV file on disk "
        "every ~30 seconds to reduce memory usage and avoid losing work if the session resets."
    )

    if st.button("🚀 Fetch Data", key="fetch_data_btn"):
        # Initialize autosave file
        autosave_path = init_autosave_file()

        # If file already exists from previous run, remove to start clean
        autosave_file = Path(autosave_path)
        if autosave_file.exists():
            try:
                autosave_file.unlink()
            except Exception as e:
                st.warning(f"Could not remove old autosave file: {e}")

        st.info(f"Autosaving to: `{autosave_path}`")

        progress = st.progress(0)
        status_placeholder = st.empty()
        autosave_placeholder = st.empty()

        total_symbols = len(df_symbols["symbol"])
        rows_buffer = []
        last_flush_time = time.time()
        flush_interval = 30  # seconds
        file_written = False

        with st.spinner("Fetching historical OHLCV data from Kite..."):
            for i, sym in enumerate(df_symbols["symbol"]):
                sym = str(sym).strip().upper()
                status_placeholder.write(f"Processing symbol **{sym}** ({i+1}/{total_symbols})")

                token = get_token(sym)

                if not token:
                    # Record error row
                    rows_buffer.append(
                        {
                            "symbol": sym,
                            "date": None,
                            "open": None,
                            "high": None,
                            "low": None,
                            "close": None,
                            "volume": None,
                            "error": "Token not found",
                        }
                    )
                else:
                    try:
                        data = kite.historical_data(
                            token,
                            from_date=datetime.combine(
                                from_date, datetime.min.time()
                            ),
                            to_date=datetime.combine(
                                to_date, datetime.max.time()
                            ),
                            interval="day",
                        )

                        for row in data:
                            rows_buffer.append(
                                {
                                    "symbol": sym,
                                    "date": row.get("date"),
                                    "open": row.get("open"),
                                    "high": row.get("high"),
                                    "low": row.get("low"),
                                    "close": row.get("close"),
                                    "volume": row.get("volume"),
                                    "error": None,
                                }
                            )

                    except Exception as e:
                        rows_buffer.append(
                            {
                                "symbol": sym,
                                "date": None,
                                "open": None,
                                "high": None,
                                "low": None,
                                "close": None,
                                "volume": None,
                                "error": str(e),
                            }
                        )

                # Decide whether to flush buffer to disk
                now = time.time()
                time_since_last_flush = now - last_flush_time
                should_flush = (
                    time_since_last_flush >= flush_interval
                    or len(rows_buffer) >= 5000
                    or i == total_symbols - 1  # always flush at the end
                )

                if should_flush:
                    file_written = flush_buffer_to_csv(
                        rows_buffer, autosave_path, file_written
                    )
                    last_flush_time = now
                    rows_flushed = len(rows_buffer)
                    autosave_placeholder.success(
                        f"Autosaved {rows_flushed} new rows at {datetime.now().strftime('%H:%M:%S')}"
                    )
                    rows_buffer = []  # clear buffer after flushing

                progress.progress((i + 1) / total_symbols)

        st.success("✅ Data fetching completed!")

        # Try to load the autosaved file for preview and download
        if autosave_file.exists():
            try:
                df_out = pd.read_csv(autosave_file)
                st.write("Preview of fetched data:")
                st.dataframe(df_out.head(500))  # don't show everything if huge

                with open(autosave_file, "rb") as f:
                    st.download_button(
                        label="📥 Download Complete OHLCV CSV",
                        data=f.read(),
                        file_name="ohlcv_output.csv",
                        mime="text/csv",
                    )
            except Exception as e:
                st.error(
                    f"Autosave file exists at `{autosave_path}` "
                    f"but could not be loaded for preview/download: {e}"
                )
                st.info("You can still access the file directly on the server/machine.")
        else:
            st.error(
                "Unexpected: autosave file does not exist. "
                "Something went wrong while writing to disk."
            )

else:
    st.info("Upload a CSV with a **symbol** column to begin.")
