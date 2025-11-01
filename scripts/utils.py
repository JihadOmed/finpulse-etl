import os
from dotenv import load_dotenv

load_dotenv(override=True)

def env(name: str, default: str = None):
    val = os.getenv(name, default)
    if val is None:
        raise RuntimeError(f"Missing environment variable: {name}")
    return val

def data_dir() -> str:
    return env("DATA_DIR", "/opt/airflow/data")