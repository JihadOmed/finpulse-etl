import pandas as pd
from sqlalchemy import create_engine, text
from .utils import data_dir, env

def _engine():
    host = env("MYSQL_HOST", "mysql")
    port = env("MYSQL_PORT", "3306")
    user = env("MYSQL_USER")
    pwd  = env("MYSQL_PASSWORD")
    db   = env("MYSQL_DB", "finance_dw")
    url = f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)

def load_data(**context):
    curated_path = os.path.join(data_dir(), "curated", "prices.parquet")
    df = pd.read_parquet(curated_path)

    engine = _engine()
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS finance_prices (
                symbol VARCHAR(20),
                date DATE,
                open DECIMAL(18,6),
                high DECIMAL(18,6),
                low DECIMAL(18,6),
                close DECIMAL(18,6),
                adj_close DECIMAL(18,6),
                volume BIGINT,
                return_1d DECIMAL(18,8),
                moving_avg_7 DECIMAL(18,6),
                moving_avg_30 DECIMAL(18,6),
                PRIMARY KEY (symbol, date)
            );
        """))

    # Replace existing rows if symbol+date already exist
    tmp_table = "finance_prices_stage"
    df.to_sql(tmp_table, engine, if_exists="replace", index=False)

    merge_sql = text("""
        INSERT INTO finance_prices
        SELECT * FROM finance_prices_stage
        ON DUPLICATE KEY UPDATE
            open=VALUES(open),
            high=VALUES(high),
            low=VALUES(low),
            close=VALUES(close),
            adj_close=VALUES(adj_close),
            volume=VALUES(volume),
            return_1d=VALUES(return_1d),
            moving_avg_7=VALUES(moving_avg_7),
            moving_avg_30=VALUES(moving_avg_30);
    """)
    with engine.begin() as conn:
        conn.execute(merge_sql)
        conn.execute(text("DROP TABLE IF EXISTS finance_prices_stage;"))

    return "loaded"