"""Runtime settings loaded from environment variables."""

import os
from pathlib import Path

from dotenv import load_dotenv # type: ignore

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

BASE_DIR = Path(__file__).resolve().parent


def _get_int(name: str, raw_value: str, minimum: int | None = None) -> int:
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(
            f"Invalid {name} value {raw_value!r}. Use an integer."
        ) from exc

    if minimum is not None and value < minimum:
        raise ValueError(
            f"Invalid {name} value {value!r}. Use a value greater than or equal to {minimum}."
        )

    return value


def _get_float(name: str, raw_value: str, minimum: float | None = None) -> float:
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise ValueError(
            f"Invalid {name} value {raw_value!r}. Use a number."
        ) from exc

    if minimum is not None and value < minimum:
        raise ValueError(
            f"Invalid {name} value {value!r}. Use a value greater than or equal to {minimum}."
        )

    return value


# mongodb settings
MONGO_URI = (os.getenv("MONGO_URI_ATLAS_MYHKSG") or "").strip()
MONGO_DATABASE = (os.getenv("MONGO_DB_ATLAS_MYHKSG") or "").strip()
COUNTRY_BY_LOCATION = {1: "HK", 3: "MY", 4: "SG"}

# mysql settings
MYSQL_HOST = (
    os.getenv("CRAWL_DB_HOST")
    or os.getenv("CRAWL_DB_READ_HOST")
    or os.getenv("crawl_db_hostname")
    or "localhost"
).strip()
MYSQL_PORT_RAW = (
    os.getenv("CRAWL_DB_PORT")
    or os.getenv("db_port")
    or "3306"
).strip()
MYSQL_USER = (
    os.getenv("CRAWL_DB_USERNAME")
    or os.getenv("CRAWL_DB_USER")
    or os.getenv("crawl_db_username")
    or ""
).strip()
MYSQL_PASSWORD = (
    os.getenv("CRAWL_DB_PASSWORD")
    or os.getenv("crawl_db_password")
    or ""
)
MYSQL_DATABASE = (
    os.getenv("CRAWL_DB_DATABASE")
    or os.getenv("CRAWL_DB_NAME")
    or os.getenv("crawl_db_name")
    or "cloudbreakr_db2"
).strip()

# gemini settings
GEMINI_API_KEY = (os.getenv("GEMINI_API_KEY_2") or "").strip()
GEMINI_EXTRACTION_MODEL = (
    os.getenv("GEMINI_EXTRACTION_MODEL")
    or "gemini-3.1-flash-lite"
).strip()
GEMINI_EMBEDDING_MODEL = (
    os.getenv("GEMINI_EMBEDDING_MODEL")
    or "gemini-embedding-2"
).strip()
EMBED_DIM_RAW = (os.getenv("EMBED_DIM") or "1536").strip()

# pricing settings
EXTRACTION_INPUT_PRICE_RAW = (
    os.getenv("EXTRACTION_INPUT_PRICE") or "0.25"
).strip()
EXTRACTION_OUTPUT_PRICE_RAW = (
    os.getenv("EXTRACTION_OUTPUT_PRICE") or "1.50"
).strip()
EMBEDDING_INPUT_PRICE_RAW = (
    os.getenv("EMBEDDING_INPUT_PRICE") or "0.20"
).strip()

# processing settings
DEFAULT_LIMIT_RAW = (os.getenv("DEFAULT_LIMIT") or "40000").strip()
DEFAULT_BATCH_SIZE_RAW = (os.getenv("DEFAULT_BATCH_SIZE") or "1000").strip()
DEFAULT_LOCATION_ID_RAW = (os.getenv("DEFAULT_LOCATION_ID") or "3").strip()
MAX_WORKERS_RAW = (os.getenv("MAX_WORKERS") or "8").strip()
REQUEST_DELAY_SECONDS_RAW = (
    os.getenv("REQUEST_DELAY_SECONDS") or "0.25"
).strip()
MAX_RETRIES_RAW = (os.getenv("MAX_RETRIES") or "5").strip()
MIN_CAPTION_LENGTH_RAW = (
    os.getenv("MIN_CAPTION_LENGTH") or "20"
).strip()


def get_mysql_port() -> int:
    return _get_int("database port", MYSQL_PORT_RAW, minimum=1)


def get_embed_dim() -> int:
    return _get_int("embedding dimension", EMBED_DIM_RAW, minimum=1)


MYSQL_PORT = get_mysql_port()
EMBED_DIM = get_embed_dim()
EXTRACTION_INPUT_PRICE = _get_float(
    "extraction input price",
    EXTRACTION_INPUT_PRICE_RAW,
    minimum=0,
)
EXTRACTION_OUTPUT_PRICE = _get_float(
    "extraction output price",
    EXTRACTION_OUTPUT_PRICE_RAW,
    minimum=0,
)
EMBEDDING_INPUT_PRICE = _get_float(
    "embedding input price",
    EMBEDDING_INPUT_PRICE_RAW,
    minimum=0,
)
DEFAULT_LIMIT = _get_int("default limit", DEFAULT_LIMIT_RAW, minimum=0)
DEFAULT_BATCH_SIZE = _get_int(
    "default batch size",
    DEFAULT_BATCH_SIZE_RAW,
    minimum=1,
)
DEFAULT_LOCATION_ID = _get_int(
    "default location id",
    DEFAULT_LOCATION_ID_RAW,
    minimum=0,
)
MAX_WORKERS = _get_int("max workers", MAX_WORKERS_RAW, minimum=1)
REQUEST_DELAY_SECONDS = _get_float(
    "request delay seconds",
    REQUEST_DELAY_SECONDS_RAW,
    minimum=0,
)
MAX_RETRIES = _get_int("max retries", MAX_RETRIES_RAW, minimum=1)
MIN_CAPTION_LENGTH = _get_int(
    "minimum caption length",
    MIN_CAPTION_LENGTH_RAW,
    minimum=0,
)

# Use dedicated step-3 outputs; do not reuse the original flow's file paths.
OUTPUT_ROOT = Path(
    os.getenv("NEW_10K_GEMINI_OUTPUT_DIR") or BASE_DIR / "output"
).resolve()
PROCESSING_VERSION = (
    f"{GEMINI_EXTRACTION_MODEL}|{GEMINI_EMBEDDING_MODEL}|{EMBED_DIM}"
)


def configure_location(location_id: int) -> None:
    global MONGO_SOURCE_COLLECTION, MONGO_TEMP_COLLECTION
    global OUTPUT_DIR, POSTS_BACKUP_FILE, SUMMARY_OUTPUT_FILE, CHECKPOINT_FILE
    if location_id not in COUNTRY_BY_LOCATION:
        raise ValueError("Location must be 1 (HK), 3 (MY), or 4 (SG)")
    country = COUNTRY_BY_LOCATION[location_id]
    MONGO_SOURCE_COLLECTION = (
        os.getenv(f"MONGO_COLL_NEW_INFLUENCER_{country}") or ""
    ).strip()
    MONGO_TEMP_COLLECTION = MONGO_SOURCE_COLLECTION
    OUTPUT_DIR = OUTPUT_ROOT / country.lower()
    POSTS_BACKUP_FILE = OUTPUT_DIR / "posts.json"
    SUMMARY_OUTPUT_FILE = OUTPUT_DIR / "processing_summary.json"
    CHECKPOINT_FILE = OUTPUT_DIR / "gemini_checkpoint.json"


configure_location(DEFAULT_LOCATION_ID)

# MongoDB fetching only; independent of Gemini retry settings.
MONGO_QUERY_TIMEOUT_MS = _get_int(
    "MongoDB query timeout", os.getenv("MONGO_QUERY_TIMEOUT_MS") or "120000", minimum=1,
)
MONGO_QUERY_ATTEMPTS = _get_int(
    "MongoDB query attempts", os.getenv("MONGO_QUERY_ATTEMPTS") or "3", minimum=1,
)
