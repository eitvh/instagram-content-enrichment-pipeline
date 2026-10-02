import os
from pathlib import Path

from dotenv import load_dotenv # type: ignore

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

# post settings
LOCATION_ID = 1  # 1 = HK, 3 = MY, 4 = SG
INFLUENCER_SOURCE = "EXTERNAL_HELPER"
POST_DATE_FROM = "2025-01-01"
POST_DATE_UNTIL = "2026-07-31"
LIMIT_PER_INFLUENCER = 10000
MAX_DOCS = 1000000   #1m
BATCH_SIZE = 500

# database settings
DB_HOST = (
    os.getenv("CRAWL_DB_HOST")
    or os.getenv("CRAWL_DB_READ_HOST")
    or os.getenv("crawl_db_hostname")
    or "localhost"
)
DB_PORT = os.getenv("CRAWL_DB_PORT") or os.getenv("db_port") or "3306"
DB_USERNAME = (
    os.getenv("CRAWL_DB_USERNAME")
    or os.getenv("crawl_db_username")
    or ""
)
DB_PASSWORD = (
    os.getenv("CRAWL_DB_PASSWORD")
    or os.getenv("crawl_db_password")
    or ""
)
DB_DATABASE = (
    os.getenv("CRAWL_DB_DATABASE")
    or os.getenv("crawl_db_name")
    or ""
)

# mongo settings
MONGO_URI = (os.getenv("MONGO_URI_ATLAS_MYHKSG") or "").strip()
MONGO_DATABASE = (os.getenv("MONGO_DB_ATLAS_MYHKSG") or "").strip()
COUNTRY_BY_LOCATION = {1: "HK", 3: "MY", 4: "SG"}
def configure_location(location_id: int) -> None:
    global LOCATION_ID, COUNTRY, POST_COLLECTION_ENV, MONGO_COLLECTION
    global CHECKPOINT_COLLECTION_ENV, MONGO_CHECKPOINT_COLLECTION

    if location_id not in COUNTRY_BY_LOCATION:
        raise ValueError("LOCATION_ID must be 1 (HK), 3 (MY), or 4 (SG)")
    LOCATION_ID = location_id
    COUNTRY = COUNTRY_BY_LOCATION[location_id]
    POST_COLLECTION_ENV = f"MONGO_COLL_NEW_INFLUENCER_{COUNTRY}"
    MONGO_COLLECTION = (os.getenv(POST_COLLECTION_ENV) or "").strip()
    CHECKPOINT_COLLECTION_ENV = f"MONGO_CHECKPOINT_NEW_POST_{COUNTRY}"
    MONGO_CHECKPOINT_COLLECTION = (
        os.getenv(CHECKPOINT_COLLECTION_ENV) or ""
    ).strip()


configure_location(LOCATION_ID)


def get_db_port() -> int:
    try:
        return int(DB_PORT)
    except ValueError as exc:
        raise ValueError(
            f"Invalid database port value {DB_PORT!r}. Use an integer like '3306'."
        ) from exc
