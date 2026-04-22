import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from io import BytesIO
from dotenv import load_dotenv


# LOGGER CONFIGURATION and setting up
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# LOAD ENV VARS
load_dotenv()

API_KEY = os.getenv("ALPHA_VANTAGE_KEY") or os.getenv("API_KEY")
ADLS_KEY = os.getenv("ADLS_KEY")

if not API_KEY:
    raise ValueError("ALPHA_VANTAGE_KEY / API_KEY missing. Check your .env file.")
if not ADLS_KEY:
    raise ValueError("ADLS_KEY missing. Check your .env file.")

logger.info("Environment variables loaded successfully.")


# TRANSFORM DAILY PRICE DATA
def transform_data(data, symbol):
    try:
        time_series = data["Time Series (Daily)"]
        df = pd.DataFrame.from_dict(time_series, orient="index")

        df = df.reset_index().rename(columns={"index": "datetime"})

        df["datetime"] = pd.to_datetime(df["datetime"])

        df = df.rename(columns={
            "1. open": "open",
            "2. high": "high",
            "3. low": "low",
            "4. close": "close",
            "5. volume": "volume"
        })

        df = df.astype({
            "open": float,
            "high": float,
            "low": float,
            "close": float,
            "volume": int
        })

        df["symbol"] = symbol
        df = df.sort_values("datetime")

        # Feature engineering
        df["daily_return"] = df["close"].pct_change() * 100
        df["price_range"] = df["high"] - df["low"]
        df["prev_close"] = df["close"].shift(1)
        df["price_direction"] = (df["close"] > df["prev_close"]).astype(int)

        df["daily_return"] = df["daily_return"].fillna(0)
        df["prev_close"] = df["prev_close"].fillna(0)

        return df

    except Exception as e:
        logger.error(f"Transform failed for {symbol}: {e}")
        return None


# FETCH WITH RETRY + ERROR HANDLING
def fetch_with_retry(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10)
            data = response.json()

            if "Time Series (Daily)" in data or "Name" in data:
                return data

            if "Information" in data:
                logger.warning("Rate limit hit. Waiting 60 seconds...")
                time.sleep(60)
                continue

            logger.error(f"Unexpected API response: {data}")
            return None

        except Exception as e:
            logger.error(f"Request failed: {e}")
            time.sleep(5)

    logger.error("Max retries reached. Skipping.")
    return None


# FETCH PRICE + OVERVIEW FOR MULTIPLE SYMBOLS
def fetch_data(symbols, api_key):
    price_frames = []
    overview_rows = []

    for i, symbol in enumerate(symbols):
        logger.info(f"Fetching price data for {symbol}...")

        url_price = (
            f"https://www.alphavantage.co/query?"
            f"function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
        )
        data_price = fetch_with_retry(url_price)

        if not data_price or "Time Series (Daily)" not in data_price:
            logger.error(f"Failed price fetch for {symbol}")
            continue

        df_price = transform_data(data_price, symbol)
        if df_price is not None:
            price_frames.append(df_price)
            logger.info(f"Price data processed for {symbol}")

        logger.info(f"Fetching overview for {symbol}...")
        url_overview = (
            f"https://www.alphavantage.co/query?"
            f"function=OVERVIEW&symbol={symbol}&apikey={api_key}"
        )
        data_overview = fetch_with_retry(url_overview)

        if not data_overview:
            logger.error(f"Failed overview fetch for {symbol}")
            continue

        overview_rows.append({
            "symbol": symbol,
            "company_name": data_overview.get("Name"),
            "sector": data_overview.get("Sector"),
            "exchange": data_overview.get("Exchange")
        })
        logger.info(f"Overview fetched for {symbol}")

        # Wait between symbols to avoid rate limit
        if i < len(symbols) - 1:
            logger.info("Waiting 15 seconds before next symbol...")
            time.sleep(15)

    if not price_frames:
        logger.error("No price frames collected.")
        return None, None

    df_prices = pd.concat(price_frames).reset_index(drop=True)
    df_overview = pd.DataFrame(overview_rows)

    return df_prices, df_overview


# UPLOAD TO ADLS GEN2
def upload_to_adls(df, container_client, path):
    try:
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        blob_client = container_client.get_blob_client(path)
        blob_client.upload_blob(buffer, overwrite=True)

        logger.info(f"Uploaded -> {path}")

    except Exception as e:
        logger.error(f"Upload failed for {path}: {e}")
        raise


# MAIN PIPELINE
def run_pipeline():
    try:
        api_key = API_KEY
        symbols = ["AAPL", "MSFT", "GOOGL", "AMZN"]

        storage_account_name = "alphadatalake"
        container_name = "stockmarket"
        storage_key = ADLS_KEY

        connection_string = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={storage_account_name};"
            f"AccountKey={storage_key};"
            f"EndpointSuffix=core.windows.net"
        )

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)

        run_date = pd.Timestamp(datetime.today().date())
        run_date_str = run_date.strftime("%Y-%m-%d")

        df_prices, df_overview = fetch_data(symbols, api_key)

        if df_prices is None:
            logger.warning(f"No price data loaded for {run_date_str}. Skipping upload.")
            return

        # Merge
        all_data = df_prices.merge(df_overview, on="symbol", how="left")

        # Convert datetime to string BEFORE writing parquet
        df_prices["datetime"] = df_prices["datetime"].astype(str)
        all_data["datetime"] = all_data["datetime"].astype(str)

        logger.info("Merged price + overview into all_data")
        logger.info(all_data.head().to_string())

        # Upload raw + refined
        upload_to_adls(
            df_prices,
            container_client,
            f"raw/stock_prices/date={run_date_str}/prices.parquet"
        )

        upload_to_adls(
            df_overview,
            container_client,
            f"raw/stock_overview/date={run_date_str}/overview.parquet"
        )

        upload_to_adls(
            all_data,
            container_client,
            f"refined/all_data/date={run_date_str}/all_data.parquet"  #  changed from curated
        )

        logger.info(f"Pipeline completed successfully for {run_date_str}.")

    except Exception as e:
        logger.critical(f"Pipeline failed: {e}")
        raise


# ENTRYPOINT
if __name__ == "__main__":
    run_pipeline()