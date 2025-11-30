import streamlit as st
from kiteconnect import KiteConnect
import pandas as pd
from datetime import datetime, timedelta

# -------------------------------
# Streamlit Page Config
# -------------------------------
st.set_page_config(page_title="Kite OHLCV Extractor", layout="centered")
st.title("📈 Kite OHLCV Extractor")
st.write("Upload a CSV with column **symbol**, fetch OHLCV data from Kite API, and download output CSV.")

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
    df = pd.DataFrame(kite.instruments("NSE"))
    return df

def get_token(symbol):
    df = load_instruments()
    row = df[df["tradingsymbol"] == symbol]
    if row.empty:
        return None
    return int(row.iloc[0]["instrument_token"])

# -------------------------------
# CSV Upload
# -------------------------------
st.subheader("2️⃣ Upload CSV with column **symbol**")

uploaded = st.file_uploader("Upload CSV", type=["csv"])

if uploaded:
    df_symbols = pd.read_csv(uploaded)

    if "symbol" not in df_symbols.columns:
        st.error("CSV must contain column: symbol")
        st.stop()

    st.write("Uploaded Symbols:")
    st.dataframe(df_symbols)

    # -------------------------------
    # Fetch OHLCV
    # -------------------------------
    st.subheader("3️⃣ Fetch OHLCV Data")

    from_date = st.date_input("From Date", datetime.now().date() - timedelta(days=30))
    to_date = st.date_input("To Date", datetime.now().date())

    if st.button("Fetch Data"):
        output_rows = []

        progress = st.progress(0)

        for i, sym in enumerate(df_symbols["symbol"]):
            token = get_token(sym)

            if not token:
                output_rows.append({
                    "symbol": sym,
                    "error": "Token not found"
                })
                progress.progress((i+1)/len(df_symbols))
                continue

            try:
                data = kite.historical_data(
                    token,
                    from_date=datetime.combine(from_date, datetime.min.time()),
                    to_date=datetime.combine(to_date, datetime.max.time()),
                    interval="day"
                )

                for row in data:
                    output_rows.append({
                        "symbol": sym,
                        "date": row["date"],
                        "open": row["open"],
                        "high": row["high"],
                        "low": row["low"],
                        "close": row["close"],
                        "volume": row["volume"]
                    })

            except Exception as e:
                output_rows.append({
                    "symbol": sym,
                    "error": str(e)
                })

            progress.progress((i+1)/len(df_symbols))

        df_out = pd.DataFrame(output_rows)

        st.success("Data fetched!")

        st.dataframe(df_out)

        # Download button
        csv_data = df_out.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="📥 Download OHLCV CSV",
            data=csv_data,
            file_name="ohlcv_output.csv",
            mime="text/csv"
        )

