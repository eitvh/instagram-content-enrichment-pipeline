import os

from dotenv import load_dotenv # type: ignore

load_dotenv()

# post settings
LOCATION_ID = 4
POST_DATE_FROM = "2025-01-01"
POST_DATE_UNTIL = "2026-07-31"
LIMIT_PER_INFLUENCER = 10000
MAX_DOCS = 0
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
MONGO_URI = (os.getenv("MONGO_URI_ATLAS_SG") or "").strip()
MONGO_DATABASE = (os.getenv("MONGO_DB_ATLAS_SG") or "").strip()
MONGO_COLLECTION = (os.getenv("MONGO_COLL_ATLAS_SG") or "").strip()
MONGO_CHECKPOINT_COLLECTION = (
    os.getenv("MONGO_CHECKPOINT_COLL") or "ig-post-sg-checkpoint"
).strip()


def get_db_port() -> int:
    try:
        return int(DB_PORT)
    except ValueError as exc:
        raise ValueError(
            f"Invalid database port value {DB_PORT!r}. Use an integer like '3306'."
        ) from exc