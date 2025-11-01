import os
import pandas as pd
import pytest
from scripts.extract_yfinance import extract_data

@pytest.fixture
def run_extraction(tmp_path, monkeypatch):
    """
    Run extract_data() using a temporary directory and small symbol list.
    """
    # Create fake symbols.txt file
    symbols_file = tmp_path / "symbols.txt"
    symbols_file.write_text("AAPL\nMSFT\n")

    # Patch config path to use temp symbols.txt
    monkeypatch.setattr("scripts.extract_yfinance._read_symbols", lambda path: ["AAPL", "MSFT"])

    # Run the extraction
    path = extract_data()

    # Ensure output file exists
    assert os.path.exists(path)
    return path


def test_extract_parquet_structure(run_extraction):
    """
    Verify that the Parquet file contains expected columns and non-empty data.
    """
    df = pd.read_parquet(run_extraction)

    expected_columns = {"date", "open", "high", "low", "close", "adj_close", "volume", "symbol"}
    assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
    assert len(df) > 0, "Extracted dataframe is empty"
    assert df["symbol"].nunique() > 0, "No symbols found"
