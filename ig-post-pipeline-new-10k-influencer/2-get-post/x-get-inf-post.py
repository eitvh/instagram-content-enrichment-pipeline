import argparse
import sys
import time
from typing import Any, Dict, List, Optional, Set, Tuple

import pymysql # type: ignore
from pymongo import MongoClient, UpdateOne # type: ignore
from pymongo.collection import Collection # type: ignore
from pymysql.cursors import DictCursor # type: ignore

import settings


def mojibake_score(text: str) -> int:
    if not text:
        return 0

    score = 0

    suspicious_sequences = (
        "ðŸ",
        "ð",
        "ï¸",
        "ï»¿",
        "Ã",
        "Â",
        "â€",
        "â€™",
        "â€œ",
        "â€�",
        "â€“",
        "â€”",
        "â€¦",
        "â„¢",
        "â",
        "æˆ",
        "æœ",
        "æ¸",
        "å®",
        "å…",
        "å°",
        "çš",
        "ä½",
    )

    for sequence in suspicious_sequences:
        score += text.count(sequence) * 3

    suspicious_characters = (
        "Ã",
        "Â",
        "â",
        "ð",
        "ï",
        "æ",
        "å",
        "ç",
        "ä",
    )

    for character in suspicious_characters:
        score += text.count(character)

    for character in text:
        codepoint = ord(character)

        if 0x80 <= codepoint <= 0x9F:
            score += 2

    return score


def misdecoded_text_to_original_bytes(text: str) -> Optional[bytes]:
    output = bytearray()

    for character in text:
        codepoint = ord(character)

        if codepoint <= 0xFF:
            output.append(codepoint)
            continue

        try:
            encoded = character.encode("cp1252", errors="strict")
        except UnicodeEncodeError:
            return None

        if len(encoded) != 1:
            return None

        output.extend(encoded)

    return bytes(output)


def repair_mojibake(text: str) -> str:
    if not text:
        return text

    best_text = text
    best_score = mojibake_score(best_text)

    if best_score == 0:
        return best_text

    for _ in range(3):
        original_bytes = misdecoded_text_to_original_bytes(best_text)

        if original_bytes is None:
            break

        try:
            candidate = original_bytes.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            break

        candidate_score = mojibake_score(candidate)

        if candidate_score >= best_score:
            break

        best_text = candidate
        best_score = candidate_score

        if best_score == 0:
            break

    return best_text


def normalize_caption(value: Any) -> str:
    if value is None:
        return ""

    if isinstance(value, bytes):
        try:
            text = value.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            text = value.decode("cp1252", errors="replace")
    else:
        text = str(value)

    return repair_mojibake(text)


def normalize_date(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None

    value = str(value).strip()

    if not value:
        return None

    if "/" in value:
        parts = value.split("/")

        if len(parts) == 3:
            day, month, year = parts
            return f"{year.zfill(4)}-{month.zfill(2)}-{day.zfill(2)}"

    return value


def format_seconds(seconds: float) -> str:
    seconds = int(seconds)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)

    if hours:
        return f"{hours}h{minutes:02d}m{seconds:02d}s"

    if minutes:
        return f"{minutes}m{seconds:02d}s"

    return f"{seconds}s"


def validate_settings() -> None:
    missing = []

    if not settings.DB_USERNAME:
        missing.append("CRAWL_DB_USERNAME")

    if not settings.DB_DATABASE:
        missing.append("CRAWL_DB_DATABASE")

    if not settings.MONGO_URI:
        missing.append("MONGO_URI_ATLAS_MYHKSG")

    if not settings.MONGO_DATABASE:
        missing.append("MONGO_DB_ATLAS_MYHKSG")

    if not settings.MONGO_COLLECTION:
        missing.append(settings.POST_COLLECTION_ENV)

    if not settings.MONGO_CHECKPOINT_COLLECTION:
        missing.append(settings.CHECKPOINT_COLLECTION_ENV)

    if missing:
        raise SystemExit(
            "Missing required environment variables: " + ", ".join(missing)
        )


def connect_mysql() -> pymysql.connections.Connection:
    return pymysql.connect(
        host=settings.DB_HOST,
        port=settings.get_db_port(),
        user=settings.DB_USERNAME,
        password=settings.DB_PASSWORD,
        database=settings.DB_DATABASE,
        charset="utf8mb4",
        use_unicode=True,
        autocommit=True,
        cursorclass=DictCursor,
        connect_timeout=8,
        read_timeout=60,
        write_timeout=60,
    )


def get_mongo_collections() -> Tuple[MongoClient, Collection, Collection]:
    client = MongoClient(
        settings.MONGO_URI,
        serverSelectionTimeoutMS=15000,
    )

    database = client[settings.MONGO_DATABASE]
    target_collection = database[settings.MONGO_COLLECTION]
    checkpoint_collection = database[settings.MONGO_CHECKPOINT_COLLECTION]

    target_collection.create_index(
        [("user_id", 1), ("post_id", 1)],
        unique=True,
    )

    checkpoint_collection.create_index(
        [
            ("locationId", 1),
            ("date_from", 1),
            ("date_until", 1),
            ("limit_per_influencer", 1),
            ("influencerId", 1),
        ],
        unique=True,
    )

    checkpoint_collection.create_index(
        [
            ("locationId", 1),
            ("date_from", 1),
            ("date_until", 1),
            ("limit_per_influencer", 1),
            ("done", 1),
        ]
    )

    return client, target_collection, checkpoint_collection


def with_retries(
    function,
    tries: int = 5,
    base_delay: float = 0.5,
    max_delay: float = 5.0,
):
    def wrapper(*args, **kwargs):
        delay = base_delay

        for attempt in range(tries):
            try:
                return function(*args, **kwargs)
            except Exception as error:
                if attempt == tries - 1:
                    raise

                print(
                    f"[warning] operation failed; retrying: {error}",
                    file=sys.stderr,
                    flush=True,
                )
                time.sleep(delay)
                delay = min(max_delay, delay * 2)

    return wrapper


@with_retries
def fetch_influencers(location_id: int) -> List[Dict[str, Any]]:
    sql = """
        SELECT
            inf.id AS influencerId,
            inf.locationId AS locationId,
            inf.ig_user_id AS ig_user_id,
            inf.identityId AS identityId,
            inf.sourceFrom AS sourceFrom,
            iu.followerCount AS followerCount
        FROM influencer AS inf
        INNER JOIN ig_user AS iu
            ON iu.id = inf.ig_user_id
        WHERE inf.deleted_at IS NULL
          AND inf.locationId = %s
          AND inf.sourceFrom = %s
          AND inf.ig_user_id IS NOT NULL
    """

    connection = connect_mysql()

    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, (location_id, settings.INFLUENCER_SOURCE))
            return cursor.fetchall() or []
    finally:
        connection.close()


@with_retries
def fetch_posts_for_influencer(
    influencer_id: int,
    date_from: Optional[str],
    date_until: Optional[str],
    limit_n: int,
) -> List[Dict[str, Any]]:
    where_parts = ["inf.id = %s", "inf.sourceFrom = %s", "inf.deleted_at IS NULL"]
    params: List[Any] = [influencer_id, settings.INFLUENCER_SOURCE]

    if date_from:
        where_parts.append("ig_post.postDate >= UNIX_TIMESTAMP(%s)")
        params.append(date_from)

    if date_until:
        where_parts.append(
            "ig_post.postDate < UNIX_TIMESTAMP(DATE_ADD(%s, INTERVAL 1 DAY))"
        )
        params.append(date_until)

    where_sql = " AND ".join(where_parts)

    sql = f"""
        SELECT
            ig_post.ig_user_id,
            ig_post.ig_post_id,
            ig_post.content AS caption,
            ig_post.likeCount,
            ig_post.commentCount,
            FROM_UNIXTIME(ig_post.postDate) AS postDate
        FROM ig_post
        INNER JOIN influencer AS inf
            ON inf.ig_user_id = ig_post.ig_user_id
        WHERE {where_sql}
        ORDER BY ig_post.postDate DESC
        LIMIT %s
    """

    params.append(int(limit_n))
    connection = connect_mysql()

    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, tuple(params))
            return cursor.fetchall() or []
    finally:
        connection.close()


def upsert_post_batch(
    collection: Collection,
    documents: List[Dict[str, Any]],
) -> Tuple[int, int, int]:
    if not documents:
        return 0, 0, 0

    operations = [
        UpdateOne(
            {
                "user_id": document["user_id"],
                "post_id": document["post_id"],
            },
            {"$set": document},
            upsert=True,
        )
        for document in documents
    ]

    result = collection.bulk_write(operations, ordered=False)

    return (
        int(result.matched_count),
        int(result.modified_count),
        len(result.upserted_ids or {}),
    )


def write_checkpoint_batch(
    checkpoint_collection: Collection,
    operations: List[UpdateOne],
) -> None:
    if not operations:
        return

    checkpoint_collection.bulk_write(operations, ordered=False)


def load_done_influencer_ids(
    checkpoint_collection: Collection,
    location_id: int,
    date_from: str,
    date_until: str,
    limit_per_influencer: int,
) -> Set[int]:
    query = {
        "locationId": int(location_id),
        "date_from": date_from,
        "date_until": date_until,
        "limit_per_influencer": int(limit_per_influencer),
        "done": True,
    }

    projection = {
        "_id": 0,
        "influencerId": 1,
    }

    result: Set[int] = set()

    for document in checkpoint_collection.find(query, projection):
        influencer_id = document.get("influencerId")

        if influencer_id is None:
            continue

        try:
            result.add(int(influencer_id))
        except (TypeError, ValueError):
            continue

    return result


def build_post_documents(
    influencer: Dict[str, Any],
    posts: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    location_id = int(influencer["locationId"])
    follower_count = influencer.get("followerCount")
    identity_id = influencer.get("identityId")
    identity_id_string = "" if identity_id is None else str(identity_id).strip()

    documents: List[Dict[str, Any]] = []

    for post in posts:
        user_id = post.get("ig_user_id")
        post_id = post.get("ig_post_id")

        if user_id is None or post_id is None:
            continue

        document = {
            "user_id": str(user_id),
            "post_id": str(post_id),
            "caption": normalize_caption(post.get("caption")),
            "locationId": location_id,
            "stats": {
                "identityId": identity_id_string,
                "followerCount": (
                    int(follower_count) if follower_count is not None else None
                ),
                "likeCount": (
                    int(post["likeCount"])
                    if post.get("likeCount") is not None
                    else None
                ),
                "commentCount": (
                    int(post["commentCount"])
                    if post.get("commentCount") is not None
                    else None
                ),
                "postDate": post.get("postDate"),
            },
        }

        documents.append(document)

    return documents


def main() -> int:
    validate_settings()

    location_id = int(settings.LOCATION_ID)
    date_from = normalize_date(settings.POST_DATE_FROM)
    date_until = normalize_date(settings.POST_DATE_UNTIL)
    limit_per_influencer = max(1, int(settings.LIMIT_PER_INFLUENCER))
    max_docs = max(0, int(settings.MAX_DOCS))
    batch_size = max(1, int(settings.BATCH_SIZE))

    if not date_from or not date_until:
        raise SystemExit("POST_DATE_FROM and POST_DATE_UNTIL must be set")

    mongo_client, target_collection, checkpoint_collection = (
        get_mongo_collections()
    )

    try:
        influencers = fetch_influencers(location_id)
        total_influencers = len(influencers)

        completed_ids = load_done_influencer_ids(
            checkpoint_collection,
            location_id=location_id,
            date_from=date_from,
            date_until=date_until,
            limit_per_influencer=limit_per_influencer,
        )

        print(
            f"[info] locationId={location_id} "
            f"sourceFrom={settings.INFLUENCER_SOURCE} "
            f"posts_collection={settings.MONGO_COLLECTION} "
            f"checkpoint_collection={settings.MONGO_CHECKPOINT_COLLECTION} "
            f"influencers={total_influencers} "
            f"completed={len(completed_ids)} "
            f"from={date_from} "
            f"until={date_until} "
            f"limit_per_influencer={limit_per_influencer} "
            f"max_docs={max_docs} "
            f"batch_size={batch_size}",
            file=sys.stderr,
            flush=True,
        )

        prepared_total = 0
        matched_total = 0
        modified_total = 0
        upserted_total = 0
        processed_influencers = 0
        skipped_influencers = 0
        checkpoint_operations: List[UpdateOne] = []
        started_at = time.time()
        last_progress_at = started_at

        for index, influencer in enumerate(influencers, start=1):
            influencer_id_raw = influencer.get("influencerId")
            location_raw = influencer.get("locationId")

            if influencer_id_raw is None or location_raw is None:
                continue

            influencer_id = int(influencer_id_raw)

            if influencer_id in completed_ids:
                skipped_influencers += 1
                continue

            if max_docs > 0 and prepared_total >= max_docs:
                break

            posts = fetch_posts_for_influencer(
                influencer_id=influencer_id,
                date_from=date_from,
                date_until=date_until,
                limit_n=limit_per_influencer,
            )

            remaining = None

            if max_docs > 0:
                remaining = max_docs - prepared_total

            influencer_was_truncated = False

            if remaining is not None and len(posts) > remaining:
                posts = posts[:remaining]
                influencer_was_truncated = True

            documents = build_post_documents(influencer, posts)

            for start in range(0, len(documents), batch_size):
                batch = documents[start : start + batch_size]
                matched_count, modified_count, upserted_count = upsert_post_batch(
                    target_collection,
                    batch,
                )

                prepared_total += len(batch)
                matched_total += matched_count
                modified_total += modified_count
                upserted_total += upserted_count

            processed_influencers += 1

            if not influencer_was_truncated:
                checkpoint_operations.append(
                    UpdateOne(
                        {
                            "locationId": location_id,
                            "date_from": date_from,
                            "date_until": date_until,
                            "limit_per_influencer": limit_per_influencer,
                            "influencerId": influencer_id,
                        },
                        {
                            "$set": {
                                "done": True,
                                "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                                "posts_processed": len(documents),
                            }
                        },
                        upsert=True,
                    )
                )

            if len(checkpoint_operations) >= 500:
                write_checkpoint_batch(
                    checkpoint_collection,
                    checkpoint_operations,
                )
                checkpoint_operations = []

            now = time.time()

            if now - last_progress_at >= 0.5:
                elapsed = format_seconds(now - started_at)
                max_docs_text = f"/{max_docs}" if max_docs > 0 else ""

                print(
                    f"\r[progress] "
                    f"idx={index}/{total_influencers} "
                    f"processed={processed_influencers} "
                    f"skipped={skipped_influencers} "
                    f"prepared={prepared_total}{max_docs_text} "
                    f"upserted={upserted_total} "
                    f"modified={modified_total} "
                    f"matched={matched_total} "
                    f"elapsed={elapsed}",
                    end="",
                    file=sys.stderr,
                    flush=True,
                )

                last_progress_at = now

            if influencer_was_truncated:
                break

        if checkpoint_operations:
            write_checkpoint_batch(
                checkpoint_collection,
                checkpoint_operations,
            )

        elapsed = format_seconds(time.time() - started_at)

        print(
            (
                f"\r[done] "
                f"influencers_total={total_influencers} "
                f"processed={processed_influencers} "
                f"skipped={skipped_influencers} "
                f"posts_prepared={prepared_total} "
                f"upserted={upserted_total} "
                f"modified={modified_total} "
                f"matched={matched_total} "
                f"elapsed={elapsed}"
            ).ljust(200),
            file=sys.stderr,
            flush=True,
        )

        print(prepared_total)
        return 0
    finally:
        mongo_client.close()


def cli() -> int:
    parser = argparse.ArgumentParser(description="Import External Helper influencer posts.")
    parser.add_argument(
        "--all-countries", action="store_true",
        help="Run MY, HK, then SG, each with its own collections and post limit.",
    )
    args = parser.parse_args()
    locations = [3, 1, 4] if args.all_countries else [settings.LOCATION_ID]

    # Check all requested configurations before starting any database work.
    for location_id in locations:
        settings.configure_location(location_id)
        validate_settings()

    for location_id in locations:
        settings.configure_location(location_id)
        print(f"[country] Starting {settings.COUNTRY}", file=sys.stderr, flush=True)
        result = main()
        if result:
            return result
    return 0


if __name__ == "__main__":
    raise SystemExit(cli())
