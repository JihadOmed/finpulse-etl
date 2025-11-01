import os
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
from .utils import data_dir


def _read_symbols(path: str) -> list:
    """
    Reads a list of ticker symbols (e.g., AAPL, MSFT) from a text file.
    Each line in the file should contain one symbol.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Symbols file not found at: {path}")
    
    with open(path, "r") as f:
        symbols = [s.strip().upper() for s in f.readlines() if s.strip()]
    
    if not symbols:
        raise ValueError("No symbols found in the symbols.txt file.")
    
    print(f"[INFO] Loaded {len(symbols)} symbols: {', '.join(symbols)}")
    return symbols


def extract_data(**context):
    """
    Extracts historical market data for multiple stock symbols
    from Yahoo Finance using the yfinance API.
    Saves the raw data to a Parquet file for the transform step.
    """
    # Path to the ticker list file
    symbols_path = "/opt/airflow/config/symbols.txt"
    symbols = _read_symbols(symbols_path)

    # Define time window for extraction (past 6 months)
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=180)

    # Directory for saving extracted data
    raw_base = os.path.join(data_dir(), "raw")
    os.makedirs(raw_base, exist_ok=True)

    all_frames = []

    print(f"[INFO] Starting extraction for {len(symbols)} symbols...")
    print(f"[INFO] Date range: {start_date} to {end_date}")

    for symbol in symbols:
        try:
            print(f"[INFO] Downloading {symbol}...")
            df = yf.download(
                symbol,
                start=start_date.isoformat(),
                end=end_date.isoformat(),
                auto_adjust=False,
                progress=False
            )

            if df.empty:
                print(f"[WARNING] No data found for {symbol}. Skipping.")
                continue

            # Reset index and add the symbol column
            df.reset_index(inplace=True)
            df["symbol"] = symbol

            all_frames.append(df)
            print(f"[SUCCESS] {symbol} data shape: {df.shape}")

        except Exception as e:
            print(f"[ERROR] Failed to download {symbol}: {e}")

    if not all_frames:
        raise RuntimeError("No data fetched for any symbols. Check your internet or symbol list.")

    # Combine all symbols into one DataFrame
    raw_df = pd.concat(all_frames, ignore_index=True)

    # Clean up column names (lowercase, replace spaces with underscores)
    raw_df.columns = [c.lower().replace(" ", "_") for c in raw_df.columns]

    # Save as Parquet for efficient Spark reading
    raw_path = os.path.join(raw_base, "extract.parquet")
    raw_df.to_parquet(raw_path, index=False)

    print(f"[INFO] Extraction complete. Saved file: {raw_path}")
    print(f"[INFO] Total rows: {len(raw_df):,}")

    # Return file path for Airflow logging
    return raw_path