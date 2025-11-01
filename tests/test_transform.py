import os
import pytest
from pyspark.sql import SparkSession
from scripts.transform_spark import transform_data
from scripts.utils import data_dir

@pytest.fixture(scope="session")
def spark():
    spark = (SparkSession.builder
                .appName("TestSpark")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate())
    yield spark
    spark.stop()


def test_transform_adds_columns(spark, tmp_path, monkeypatch):
    """
    Ensure transformation adds new calculated columns.
    """
    # Prepare mock raw data
    data = [
        ("AAPL", "2025-01-01", 100, 101, 99, 100.5, 100.5, 1_000_000),
        ("AAPL", "2025-01-02", 101, 102, 99, 101.5, 101.5, 1_100_000),
    ]
    columns = ["symbol", "date", "open", "high", "low", "close", "adj_close", "volume"]
    df = spark.createDataFrame(data, columns)

    raw_dir = tmp_path / "raw"
    os.makedirs(raw_dir, exist_ok=True)
    df.write.parquet(str(raw_dir / "extract.parquet"))

    # Patch data_dir to use temp folder
    monkeypatch.setattr("scripts.utils.data_dir", lambda: str(tmp_path))

    # Run transformation
    transform_data()

    curated_path = os.path.join(tmp_path, "curated", "prices.parquet")
    assert os.path.exists(curated_path), "Curated file not created"

    df_result = spark.read.parquet(curated_path)
    for col in ["return_1d", "moving_avg_7", "moving_avg_30"]:
        assert col in df_result.columns, f"{col} column missing"
