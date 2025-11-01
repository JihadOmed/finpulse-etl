import pandas as pd
import pytest
from sqlalchemy import create_engine
from scripts.load_mysql import load_data
from scripts.utils import data_dir

@pytest.fixture
def sqlite_engine(tmp_path, monkeypatch):
    """
    Use SQLite in-memory DB to simulate MySQL during tests.
    """
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    # Patch _engine() in load_mysql.py
    monkeypatch.setattr("scripts.load_mysql._engine", lambda: engine)
    monkeypatch.setattr("scripts.utils.data_dir", lambda: str(tmp_path))

    return engine


def test_load_inserts_data(sqlite_engine, tmp_path):
    """
    Ensure load_data writes data to database.
    """
    # Create fake curated parquet
    data = {
        "symbol": ["AAPL", "MSFT"],
        "date": ["2025-01-01", "2025-01-02"],
        "open": [100, 200],
        "high": [105, 205],
        "low": [99, 198],
        "close": [102, 203],
        "adj_close": [102, 203],
        "volume": [1_000_000, 2_000_000],
        "return_1d": [0.01, 0.02],
        "moving_avg_7": [101, 202],
        "moving_avg_30": [100, 201],
    }
    df = pd.DataFrame(data)
    curated_dir = tmp_path / "curated"
    curated_dir.mkdir(exist_ok=True)
    df.to_parquet(curated_dir / "prices.parquet")

    # Run load_data()
    load_data()

    # Validate data was inserted
    with sqlite_engine.connect() as conn:
        result = conn.execute("SELECT COUNT(*) FROM finance_prices").scalar()
        assert result == 2, "Data not inserted correctly"
