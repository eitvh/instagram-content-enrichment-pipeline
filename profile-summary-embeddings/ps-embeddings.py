import os
import sys
import time
import json
import argparse
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
import pymysql
from pymysql.cursors import DictCursor
from pymongo import MongoClient, ReplaceOne, ASCENDING
from google import genai
from google.genai import types as genai_types

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

MYSQL_HOST = os.getenv("CRAWL_DB_HOST", "localhost")
MYSQL_PORT = int(os.getenv("CRAWL_DB_PORT", "3306"))
MYSQL_DATABASE = os.getenv("CRAWL_DB_DATABASE", "cloudbreakr_db2")
MYSQL_USERNAME = os.getenv("CRAWL_DB_USERNAME", "cloudbreakr")
MYSQL_PASSWORD = os.getenv("CRAWL_DB_PASSWORD", "")
MYSQL_TABLE = os.getenv("CRAWL_DB_PROFILE_SUMMARY_TABLE", "profile_summary")

MONGO_URI = os.getenv("MONGO_URI_ATLAS_PROFILE_SUMMARY", "")
MONGO_DATABASE = os.getenv("MONGO_DB_ATLAS_PROFILE_SUMMARY", "ai-vector-search")
MONGO_COLLECTION = os.getenv("MONGO_COLL_ATLAS_PROFILE_SUMMARY", "profile-summary")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_EMBEDDING_MODEL = os.getenv("GEMINI_EMBEDDING_MODEL", "gemini-embedding-2")
GEMINI_EMBEDDING_TASK_TYPE = os.getenv("GEMINI_EMBEDDING_TASK_TYPE", "SEMANTIC_SIMILARITY")
EMBED_DIM = int(os.getenv("EMBED_DIM", "1536"))
EMBEDDING_INPUT_PRICE = float(os.getenv("EMBEDDING_INPUT_PRICE", "0.20"))

DEFAULT_BATCH_SIZE = int(os.getenv("PROFILE_SUMMARY_BATCH_SIZE", "500"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
REQUEST_DELAY_SECONDS = float(os.getenv("REQUEST_DELAY_SECONDS", "0.25"))

CHECKPOINT_FILE = os.path.abspath(
    os.getenv(
        "PROFILE_SUMMARY_EMBEDDINGS_CHECKPOINT",
        os.path.join(BASE_DIR, "ps-embeddings-checkpoint.json"),
    )
)

PROCESSING_VERSION = f"{GEMINI_EMBEDDING_MODEL}|{GEMINI_EMBEDDING_TASK_TYPE}|{EMBED_DIM}"

_usage_lock = threading.Lock()
_usage = {"embedding_input_tokens": 0}


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Embed MySQL profile_summary.summary into MongoDB Atlas")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    parser.add_argument("--reset-checkpoint", action="store_true")
    parser.add_argument("--reembed-existing", action="store_true")
    return parser.parse_args()


def validate_config() -> None:
    missing = []
    if not MYSQL_HOST:
        missing.append("CRAWL_DB_HOST")
    if not MYSQL_USERNAME:
        missing.append("CRAWL_DB_USERNAME")
    if not MYSQL_PASSWORD:
        missing.append("CRAWL_DB_PASSWORD")
    if not MONGO_URI:
        missing.append("MONGO_URI_ATLAS_PROFILE_SUMMARY")
    if not GEMINI_API_KEY:
        missing.append("GEMINI_API_KEY")
    if missing:
        raise SystemExit(f"Missing required environment variables: {', '.join(missing)}")


def connect_mysql() -> pymysql.connections.Connection:
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USERNAME,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        charset="utf8mb4",
        autocommit=True,
        use_unicode=True,
        cursorclass=DictCursor,
        connect_timeout=15,
        read_timeout=120,
        write_timeout=120,
    )


def connect_mongodb() -> Tuple[MongoClient, Any]:
    client = MongoClient(MONGO_URI)
    client.admin.command("ping")
    collection = client[MONGO_DATABASE][MONGO_COLLECTION]
    collection.create_index([("inf_id", ASCENDING)], unique=True, name="inf_id_unique")
    return client, collection


def normalize_summary(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        text = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    elif isinstance(value, bytes):
        text = value.decode("utf-8", errors="replace")
    else:
        text = str(value)
    text = text.replace("\x00", "").replace("\r\n", "\n").replace("\r", "\n")
    return text.strip()


def fetch_mysql_batch(last_mysql_id: int, batch_size: int) -> List[Dict[str, Any]]:
    query = f"""
        SELECT id, inf_id, summary
        FROM `{MYSQL_TABLE}`
        WHERE id > %s
          AND inf_id IS NOT NULL
          AND summary IS NOT NULL
        ORDER BY id ASC
        LIMIT %s
    """
    connection = connect_mysql()
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, (last_mysql_id, batch_size))
            rows = cursor.fetchall() or []
    finally:
        connection.close()
    result = []
    for row in rows:
        result.append(
            {
                "id": int(row["id"]),
                "inf_id": int(row["inf_id"]),
                "summary": normalize_summary(row.get("summary")),
            }
        )
    return result


def fetch_existing_inf_ids(collection: Any, inf_ids: List[int]) -> set:
    if not inf_ids:
        return set()
    query = {
        "inf_id": {"$in": inf_ids},
        "stats.embedding_model": GEMINI_EMBEDDING_MODEL,
        f"embedding.{EMBED_DIM - 1}": {"$exists": True},
        f"embedding.{EMBED_DIM}": {"$exists": False},
    }
    return {int(doc["inf_id"]) for doc in collection.find(query, {"_id": 0, "inf_id": 1})}


def is_retryable_error(message: str) -> bool:
    lowered = message.lower()
    keys = [
        "resource_exhausted",
        "429",
        "quota",
        "rate",
        "too many requests",
        "timeout",
        "deadline",
        "unavailable",
        "connection",
        "temporarily",
        "503",
        "500",
    ]
    return any(key in lowered for key in keys)


def sleep_with_backoff(attempt: int) -> None:
    time.sleep(min(60.0, 1.5 * (2 ** attempt)))


def track_embedding_usage(response: Any, text: str) -> None:
    usage = getattr(response, "usage_metadata", None)
    tokens = 0
    if usage is not None:
        tokens = int(
            getattr(usage, "prompt_token_count", 0)
            or getattr(usage, "total_token_count", 0)
            or 0
        )
    if tokens <= 0:
        tokens = max(1, len(text) // 3)
    with _usage_lock:
        _usage["embedding_input_tokens"] += tokens


def get_usage_snapshot() -> Dict[str, int]:
    with _usage_lock:
        return {"embedding_input_tokens": int(_usage["embedding_input_tokens"])}


def restore_usage(values: Dict[str, Any]) -> None:
    with _usage_lock:
        _usage["embedding_input_tokens"] = int(values.get("embedding_input_tokens", 0) or 0)


def get_cost_summary() -> Dict[str, Any]:
    usage = get_usage_snapshot()
    tokens = usage["embedding_input_tokens"]
    return {
        "embedding_model": GEMINI_EMBEDDING_MODEL,
        "embedding_input_tokens": tokens,
        "embedding_cost_usd": round(tokens / 1_000_000 * EMBEDDING_INPUT_PRICE, 6),
    }


def embed_text(client: genai.Client, text: str) -> List[float]:
    for attempt in range(MAX_RETRIES):
        try:
            if REQUEST_DELAY_SECONDS > 0:
                time.sleep(REQUEST_DELAY_SECONDS)
            response = client.models.embed_content(
                model=GEMINI_EMBEDDING_MODEL,
                contents=[text],
                config=genai_types.EmbedContentConfig(
                    task_type=GEMINI_EMBEDDING_TASK_TYPE,
                    output_dimensionality=EMBED_DIM,
                ),
            )
            embeddings = getattr(response, "embeddings", None)
            if not embeddings or not isinstance(embeddings, list):
                raise RuntimeError("No embeddings returned")
            values = getattr(embeddings[0], "values", None)
            if not values or not isinstance(values, list):
                raise RuntimeError("Missing embedding values")
            vector = [float(value) for value in values]
            if len(vector) != EMBED_DIM:
                raise RuntimeError(f"Unexpected embedding dimension: {len(vector)}")
            track_embedding_usage(response, text)
            return vector
        except Exception as error:
            message = str(error)
            if attempt < MAX_RETRIES - 1 and is_retryable_error(message):
                sleep_with_backoff(attempt)
                continue
            if attempt < MAX_RETRIES - 1:
                sleep_with_backoff(attempt)
                continue
            raise RuntimeError(message) from error
    raise RuntimeError("Embedding failed")


def process_embedding_batch(
    client: genai.Client,
    rows: List[Dict[str, Any]],
    workers: int,
) -> Tuple[List[Tuple[int, Dict[str, Any]]], List[int]]:
    documents: List[Tuple[int, Dict[str, Any]]] = []
    failed_mysql_ids: List[int] = []
    total = len(rows)
    completed = 0
    start = time.time()

    def process_one(row: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        vector = embed_text(client, row["summary"])
        document = {
            "inf_id": row["inf_id"],
            "embedding": vector,
            "stats": {
                "embedding_at": time.time(),
                "embedding_model": GEMINI_EMBEDDING_MODEL,
            },
        }
        return row["id"], document

    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        future_map = {executor.submit(process_one, row): row for row in rows}
        for future in as_completed(future_map):
            row = future_map[future]
            completed += 1
            try:
                documents.append(future.result())
            except Exception as error:
                failed_mysql_ids.append(row["id"])
                print(f"[embed-error] mysql_id={row['id']} inf_id={row['inf_id']} {str(error)[:300]}")
            if completed % 10 == 0 or completed == total:
                elapsed = time.time() - start
                rate = completed / elapsed if elapsed > 0 else 0.0
                remaining = total - completed
                eta = remaining / rate if rate > 0 else 0.0
                cost = get_cost_summary()
                print(
                    f"[embed] {completed}/{total} | {rate:.2f}/s | ETA {eta:.0f}s | "
                    f"errors={len(failed_mysql_ids)} | tokens={cost['embedding_input_tokens']} | "
                    f"cost=${cost['embedding_cost_usd']:.6f}"
                )

    documents.sort(key=lambda item: item[0])
    failed_mysql_ids.sort()
    return documents, failed_mysql_ids


def write_mongodb_documents(
    collection: Any,
    documents: List[Tuple[int, Dict[str, Any]]],
) -> List[int]:
    if not documents:
        return []
    operations = [
        ReplaceOne({"inf_id": document["inf_id"]}, document, upsert=True)
        for _, document in documents
    ]
    try:
        result = collection.bulk_write(operations, ordered=True)
        print(
            f"[mongodb] matched={result.matched_count} modified={result.modified_count} "
            f"upserted={result.upserted_count}"
        )
        return []
    except Exception as error:
        print(f"[mongodb] bulk write failed, retrying individually: {str(error)[:300]}")
        failed_mysql_ids = []
        for mysql_id, document in documents:
            try:
                collection.replace_one({"inf_id": document["inf_id"]}, document, upsert=True)
            except Exception as single_error:
                failed_mysql_ids.append(mysql_id)
                print(
                    f"[mongodb-error] mysql_id={mysql_id} inf_id={document['inf_id']} "
                    f"{str(single_error)[:300]}"
                )
        return failed_mysql_ids


def new_checkpoint(limit: int) -> Dict[str, Any]:
    return {
        "checkpoint_version": 1,
        "mysql_database": MYSQL_DATABASE,
        "mysql_table": MYSQL_TABLE,
        "mongo_database": MONGO_DATABASE,
        "mongo_collection": MONGO_COLLECTION,
        "processing_version": PROCESSING_VERSION,
        "requested_limit": int(limit),
        "last_mysql_id": 0,
        "total_completed": 0,
        "total_embedded": 0,
        "total_skipped_empty": 0,
        "total_skipped_existing": 0,
        "total_errors": 0,
        "batch_number": 0,
        "processing_time_seconds": 0.0,
        "usage": get_usage_snapshot(),
        "cost": get_cost_summary(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def load_checkpoint(limit: int, reset: bool) -> Dict[str, Any]:
    if reset and os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
    if not os.path.exists(CHECKPOINT_FILE):
        return new_checkpoint(limit)
    try:
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as file:
            state = json.load(file)
    except Exception:
        return new_checkpoint(limit)
    expected = {
        "mysql_database": MYSQL_DATABASE,
        "mysql_table": MYSQL_TABLE,
        "mongo_database": MONGO_DATABASE,
        "mongo_collection": MONGO_COLLECTION,
        "processing_version": PROCESSING_VERSION,
    }
    for key, value in expected.items():
        if state.get(key) != value:
            return new_checkpoint(limit)
    state["requested_limit"] = int(limit)
    return state


def save_checkpoint(state: Dict[str, Any]) -> None:
    state["usage"] = get_usage_snapshot()
    state["cost"] = get_cost_summary()
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    directory = os.path.dirname(CHECKPOINT_FILE)
    if directory:
        os.makedirs(directory, exist_ok=True)
    temp_file = CHECKPOINT_FILE + ".tmp"
    with open(temp_file, "w", encoding="utf-8") as file:
        json.dump(state, file, ensure_ascii=False, indent=2)
    os.replace(temp_file, CHECKPOINT_FILE)


def process() -> None:
    args = parse_arguments()
    validate_config()

    limit = max(0, int(args.limit))
    batch_size = max(1, int(args.batch_size))
    workers = max(1, int(args.workers))

    checkpoint = load_checkpoint(limit, args.reset_checkpoint)
    restore_usage(checkpoint.get("usage", {}))

    gemini_client = genai.Client(api_key=GEMINI_API_KEY)
    mongo_client, mongo_collection = connect_mongodb()

    total_completed = int(checkpoint.get("total_completed", 0) or 0)
    total_embedded = int(checkpoint.get("total_embedded", 0) or 0)
    total_skipped_empty = int(checkpoint.get("total_skipped_empty", 0) or 0)
    total_skipped_existing = int(checkpoint.get("total_skipped_existing", 0) or 0)
    total_errors = int(checkpoint.get("total_errors", 0) or 0)
    last_mysql_id = int(checkpoint.get("last_mysql_id", 0) or 0)
    batch_number = int(checkpoint.get("batch_number", 0) or 0)
    previous_elapsed = float(checkpoint.get("processing_time_seconds", 0.0) or 0.0)
    started_at = time.time()

    print("=" * 72)
    print("Profile Summary Embeddings")
    print(f"MySQL:     {MYSQL_DATABASE}.{MYSQL_TABLE}")
    print(f"MongoDB:   {MONGO_DATABASE}.{MONGO_COLLECTION}")
    print(f"Model:     {GEMINI_EMBEDDING_MODEL}")
    print(f"Task type: {GEMINI_EMBEDDING_TASK_TYPE}")
    print(f"Dimension: {EMBED_DIM}")
    print(f"Workers:   {workers}")
    print(f"Batch:     {batch_size}")
    print(f"Limit:     {'all' if limit == 0 else limit}")
    print(f"Checkpoint:{CHECKPOINT_FILE}")
    print(f"Resume ID: {last_mysql_id}")
    print("=" * 72)

    try:
        while limit == 0 or total_completed < limit:
            remaining = batch_size if limit == 0 else min(batch_size, limit - total_completed)
            if remaining <= 0:
                break

            rows = fetch_mysql_batch(last_mysql_id, remaining)
            if not rows:
                print("[mysql] No more rows to process")
                break

            batch_number += 1
            print(
                f"[batch {batch_number}] mysql_id={rows[0]['id']}..{rows[-1]['id']} "
                f"rows={len(rows)}"
            )

            empty_ids = {row["id"] for row in rows if not row["summary"]}
            nonempty_rows = [row for row in rows if row["id"] not in empty_ids]

            existing_ids = set()
            if nonempty_rows and not args.reembed_existing:
                existing_inf_ids = fetch_existing_inf_ids(
                    mongo_collection,
                    list({row["inf_id"] for row in nonempty_rows}),
                )
                existing_ids = {row["id"] for row in nonempty_rows if row["inf_id"] in existing_inf_ids}

            rows_to_embed = [
                row
                for row in nonempty_rows
                if row["id"] not in existing_ids
            ]

            print(
                f"[batch {batch_number}] embed={len(rows_to_embed)} "
                f"empty={len(empty_ids)} existing={len(existing_ids)}"
            )

            documents, embedding_failed_ids = process_embedding_batch(
                gemini_client,
                rows_to_embed,
                workers,
            )

            mongo_failed_ids = write_mongodb_documents(mongo_collection, documents)
            failed_ids = sorted(set(embedding_failed_ids) | set(mongo_failed_ids))

            total_embedded += len(documents) - len(mongo_failed_ids)
            total_skipped_empty += len(empty_ids)
            total_skipped_existing += len(existing_ids)
            total_errors += len(failed_ids)

            if failed_ids:
                earliest_failed_id = failed_ids[0]
                advanced_rows = [row for row in rows if row["id"] < earliest_failed_id]
                if advanced_rows:
                    last_mysql_id = advanced_rows[-1]["id"]
                    total_completed += len(advanced_rows)
                checkpoint["last_mysql_id"] = last_mysql_id
                checkpoint["total_completed"] = total_completed
                checkpoint["total_embedded"] = total_embedded
                checkpoint["total_skipped_empty"] = total_skipped_empty
                checkpoint["total_skipped_existing"] = total_skipped_existing
                checkpoint["total_errors"] = total_errors
                checkpoint["batch_number"] = batch_number
                checkpoint["processing_time_seconds"] = previous_elapsed + (time.time() - started_at)
                save_checkpoint(checkpoint)
                print(
                    f"[stop] Batch has failures. Checkpoint saved before mysql_id={earliest_failed_id}. "
                    f"Restart the script to retry from that point."
                )
                break

            last_mysql_id = rows[-1]["id"]
            total_completed += len(rows)

            checkpoint["last_mysql_id"] = last_mysql_id
            checkpoint["total_completed"] = total_completed
            checkpoint["total_embedded"] = total_embedded
            checkpoint["total_skipped_empty"] = total_skipped_empty
            checkpoint["total_skipped_existing"] = total_skipped_existing
            checkpoint["total_errors"] = total_errors
            checkpoint["batch_number"] = batch_number
            checkpoint["processing_time_seconds"] = previous_elapsed + (time.time() - started_at)
            save_checkpoint(checkpoint)

            cost = get_cost_summary()
            print(
                f"[checkpoint] completed={total_completed} embedded={total_embedded} "
                f"empty={total_skipped_empty} existing={total_skipped_existing} errors={total_errors} "
                f"last_mysql_id={last_mysql_id} cost=${cost['embedding_cost_usd']:.6f}"
            )
    finally:
        checkpoint["last_mysql_id"] = last_mysql_id
        checkpoint["total_completed"] = total_completed
        checkpoint["total_embedded"] = total_embedded
        checkpoint["total_skipped_empty"] = total_skipped_empty
        checkpoint["total_skipped_existing"] = total_skipped_existing
        checkpoint["total_errors"] = total_errors
        checkpoint["batch_number"] = batch_number
        checkpoint["processing_time_seconds"] = previous_elapsed + (time.time() - started_at)
        save_checkpoint(checkpoint)
        mongo_client.close()

    cost = get_cost_summary()
    print("=" * 72)
    print("Done")
    print(f"Completed source rows: {total_completed}")
    print(f"Embedded:             {total_embedded}")
    print(f"Skipped empty:         {total_skipped_empty}")
    print(f"Skipped existing:      {total_skipped_existing}")
    print(f"Errors:                {total_errors}")
    print(f"Last MySQL id:         {last_mysql_id}")
    print(f"Embedding tokens:      {cost['embedding_input_tokens']}")
    print(f"Estimated cost:        ${cost['embedding_cost_usd']:.6f}")
    print(f"Checkpoint:            {CHECKPOINT_FILE}")
    print("=" * 72)


if __name__ == "__main__":
    try:
        process()
    except KeyboardInterrupt:
        print("Interrupted")
        sys.exit(130)
    except Exception as error:
        print(f"Fatal: {error}")
        sys.exit(1)