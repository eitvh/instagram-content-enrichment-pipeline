import sys
import time
from typing import Callable, List, Optional, TypeVar

import pymysql # type: ignore
from pymysql.cursors import DictCursor # type: ignore

from settings import (
    DB_DATABASE,
    DB_HOST,
    DB_PASSWORD,
    DB_USERNAME,
    LIMIT_PER_INFLUENCER,
    LOCATION_ID,
    POST_DATE_FROM,
    POST_DATE_UNTIL,
    get_db_port,
)

T = TypeVar("T")


def _normalize_date(value: Optional[str]) -> Optional[str]:
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


def _connect() -> pymysql.connections.Connection:
    return pymysql.connect(
        host=DB_HOST,
        port=get_db_port(),
        user=DB_USERNAME,
        passwd=DB_PASSWORD,
        db=DB_DATABASE,
        charset="utf8mb4",
        autocommit=True,
        use_unicode=True,
        cursorclass=DictCursor,
        connect_timeout=8,
    )


def _with_retries(
    function: Callable[..., T],
    tries: int = 5,
    base_delay: float = 0.5,
    max_delay: float = 5.0,
) -> Callable[..., T]:
    def _wrapper(*args, **kwargs) -> T:
        delay = base_delay

        for attempt in range(tries):
            try:
                return function(*args, **kwargs)
            except Exception:
                if attempt == tries - 1:
                    raise

                time.sleep(delay)
                delay = min(max_delay, delay * 2)

        raise RuntimeError("Retry loop ended unexpectedly.")

    return _wrapper


@_with_retries
def fetch_influencer_ids(location_id: int) -> List[int]:
    # match the get-list.py influencer population
    sql = """
        SELECT inf.id AS influencerId
        FROM influencer AS inf
        INNER JOIN ig_user AS iu
            ON iu.id = inf.ig_user_id
        WHERE inf.deleted_at IS NULL
          AND inf.locationId = %s
          AND inf.ig_user_id IS NOT NULL
    """

    conn = _connect()

    try:
        with conn.cursor() as cur:
            cur.execute(sql, (location_id,))
            rows = cur.fetchall() or []

            return [
                int(row["influencerId"])
                for row in rows
                if row.get("influencerId") is not None
            ]
    finally:
        conn.close()


@_with_retries
def count_posts_for_influencer(
    influencer_id: int,
    date_from: Optional[str],
    date_until: Optional[str],
    limit_n: int,
) -> int:
    where_parts = ["inf.id = %s"]
    params: List[object] = [influencer_id]

    if date_from:
        where_parts.append("ig_post.postDate >= UNIX_TIMESTAMP(%s)")
        params.append(date_from)

    if date_until:
        where_parts.append("ig_post.postDate <= UNIX_TIMESTAMP(%s)")
        params.append(date_until)

    where_sql = " AND ".join(where_parts)

    sql_count = f"""
        SELECT COUNT(*) AS cnt
        FROM (
            SELECT ig_post.ig_post_id
            FROM ig_post
            INNER JOIN influencer AS inf
                ON inf.ig_user_id = ig_post.ig_user_id
            WHERE {where_sql}
            GROUP BY ig_post.ig_post_id
            ORDER BY ig_post.postDate DESC
            LIMIT %s
        ) AS t
    """

    params.append(limit_n)

    conn = _connect()

    try:
        with conn.cursor() as cur:
            cur.execute("SET NAMES latin1")
            cur.execute(sql_count, tuple(params))

            row = cur.fetchone() or {}
            return int(row.get("cnt", 0))
    finally:
        conn.close()


def main() -> None:
    location_id = int(LOCATION_ID)
    date_from = _normalize_date(POST_DATE_FROM)
    date_until = _normalize_date(POST_DATE_UNTIL)
    limit_n = int(LIMIT_PER_INFLUENCER)

    influencer_ids = fetch_influencer_ids(location_id)
    influencer_count = len(influencer_ids)

    print(
        f"[info] locationId={location_id} "
        f"influencers(with ig_user)={influencer_count} "
        f"from={date_from or '-'} "
        f"until={date_until or '-'} "
        f"limit_per_influencer={limit_n}",
        file=sys.stderr,
        flush=True,
    )

    total_posts = 0

    for index, influencer_id in enumerate(influencer_ids, start=1):
        post_count = count_posts_for_influencer(
            influencer_id=influencer_id,
            date_from=date_from,
            date_until=date_until,
            limit_n=limit_n,
        )

        total_posts += post_count

        if index == 1 or index % 10 == 0 or index == influencer_count:
            print(
                f"[progress] {index}/{influencer_count} "
                f"total_posts_so_far={total_posts}",
                file=sys.stderr,
                flush=True,
            )

    print(influencer_count)
    print(total_posts)


if __name__ == "__main__":
    main()