"""
Embeddings Testing Script

Reads post IDs from MongoDB Atlas `ig-post` collection (existing records),
fetches caption content from MySQL `ig_post`, extracts rich metadata plus
caption embedding via Gemini, and stores results in a TEMPORARY MongoDB
collection (`ig-post-embeddings-test`) that mirrors the `ig-post` schema.

The temp collection uses the same field structure as `ig-post` but with new
embedding + metadata fields, so you can test $vectorSearch without touching
production data.

New fields per document:
  caption, tags, brand_name, associated_brand, associated_mention,
  hashtags, is_sponsorship, embedding, embedding_model, embedding_updated_at,
  gemini_extraction_model, gemini_processing_updated_at

Usage:
    # Random sample of 1000 posts, process and upsert to temp collection
    python run-temp.py

    # Process 100 posts with 5 workers
    python run-temp.py --limit 100 --workers 5

    # Skip posts already in the temp collection (incremental mode)
    python run-temp.py --skip-existing

    # Read fresh posts from MySQL directly
    python run-temp.py --source mysql --location-id 1 --limit 2000
"""

import os
import sys
import time
import json
import re
import argparse
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Tuple, Set

from dotenv import load_dotenv # type: ignore
import pymongo # type: ignore
from pymongo import MongoClient, UpdateOne # type: ignore
from bson import ObjectId, json_util # type: ignore
from google import genai
from google.genai import types as genai_types # type: ignore

load_dotenv()


# Configuration

# MongoDB Atlas
MONGO_URI = os.getenv("MONGO_URI_ATLAS", "")
MONGO_DATABASE = os.getenv("MONGO_DB_NAME", "ai-vector-search")
MONGO_SOURCE_COLLECTION = "ig-post"
MONGO_TEMP_COLLECTION = "ig-post-embeddings-test"

# MySQL
MYSQL_HOST = os.getenv("CRAWL_DB_HOST", "localhost")
MYSQL_PORT = int(os.getenv("CRAWL_DB_PORT", "3306"))
MYSQL_USER = os.getenv("CRAWL_DB_USERNAME", "")
MYSQL_PASSWORD = os.getenv("CRAWL_DB_PASSWORD", "")
MYSQL_DATABASE = os.getenv("CRAWL_DB_DATABASE", "cloudbreakr_db2")

# Gemini
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_EXTRACTION_MODEL = os.getenv("GEMINI_EXTRACTION_MODEL", "gemini-3.1-flash-lite")
GEMINI_EMBEDDING_MODEL = os.getenv("GEMINI_EMBEDDING_MODEL", "gemini-embedding-2")
EMBED_DIM = int(os.getenv("EMBED_DIM", "1536"))

# Per-model pricing (USD per 1M tokens)
# Update these if pricing changes: https://ai.google.dev/pricing
EXTRACTION_INPUT_PRICE = float(os.getenv("EXTRACTION_INPUT_PRICE", "0.25"))
EXTRACTION_OUTPUT_PRICE = float(os.getenv("EXTRACTION_OUTPUT_PRICE", "1.50"))
EMBEDDING_INPUT_PRICE = float(os.getenv("EMBEDDING_INPUT_PRICE", "0.20"))

# Processing
DEFAULT_LIMIT = 5   # post per run (always modify this)
DEFAULT_BATCH_SIZE = 1000   # post per batch
DEFAULT_LOCATION_ID = 1
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8")) # can set max workers to 8
REQUEST_DELAY_SECONDS = float(os.getenv("REQUEST_DELAY_SECONDS", "0.25"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))

# Minimum caption length to consider meaningful
MIN_CAPTION_LENGTH = 20

# Local output
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
POSTS_BACKUP_FILE = os.path.join(OUTPUT_DIR, "posts.json")
PROCESSED_POSTID_FILE = os.path.join(OUTPUT_DIR, "processed_postid.json")
TEMP_RECORDS_FILE = os.path.join(OUTPUT_DIR, "temp_generated_records.json")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, "checkpoint.json")
REALTIME_SUMMARY_FILE = os.path.join(OUTPUT_DIR, "realtime_summary.json")
MONGO_FLUSH_SIZE = int(os.getenv("MONGO_FLUSH_SIZE", "20"))



# Usage tracking (thread-safe)

_usage_lock = threading.Lock()
_usage = {
    "extraction_input_tokens": 0,
    "extraction_output_tokens": 0,
    "embedding_input_tokens": 0,
}
_stop_event = threading.Event()
_stop_lock = threading.Lock()
_stop_reason = ""


class PipelineStop(Exception):
    pass


class PostProcessingError(Exception):
    pass


def now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def track_usage(category: str, input_tokens: int, output_tokens: int = 0) -> None:
    with _usage_lock:
        if input_tokens:
            _usage[f"{category}_input_tokens"] = _usage.get(f"{category}_input_tokens", 0) + int(input_tokens)
        if output_tokens:
            _usage[f"{category}_output_tokens"] = _usage.get(f"{category}_output_tokens", 0) + int(output_tokens)


def get_usage_snapshot() -> Dict[str, int]:
    with _usage_lock:
        return {
            "extraction_input_tokens": int(_usage.get("extraction_input_tokens", 0)),
            "extraction_output_tokens": int(_usage.get("extraction_output_tokens", 0)),
            "embedding_input_tokens": int(_usage.get("embedding_input_tokens", 0)),
        }


def restore_usage(values: Dict[str, Any]) -> None:
    with _usage_lock:
        _usage["extraction_input_tokens"] = int(values.get("extraction_input_tokens", 0) or 0)
        _usage["extraction_output_tokens"] = int(values.get("extraction_output_tokens", 0) or 0)
        _usage["embedding_input_tokens"] = int(values.get("embedding_input_tokens", 0) or 0)


def get_usage_summary() -> Dict[str, Any]:
    usage = get_usage_snapshot()
    extract_in = usage["extraction_input_tokens"]
    extract_out = usage["extraction_output_tokens"]
    embed_in = usage["embedding_input_tokens"]
    extract_cost = (
        extract_in / 1_000_000 * EXTRACTION_INPUT_PRICE
        + extract_out / 1_000_000 * EXTRACTION_OUTPUT_PRICE
    )
    embed_cost = embed_in / 1_000_000 * EMBEDDING_INPUT_PRICE
    return {
        "extraction_model": GEMINI_EXTRACTION_MODEL,
        "extraction_input_tokens": extract_in,
        "extraction_output_tokens": extract_out,
        "extraction_total_tokens": extract_in + extract_out,
        "extraction_cost_usd": round(extract_cost, 6),
        "embedding_model": GEMINI_EMBEDDING_MODEL,
        "embedding_input_tokens": embed_in,
        "embedding_cost_usd": round(embed_cost, 6),
        "combined_tokens": extract_in + extract_out + embed_in,
        "total_cost_usd": round(extract_cost + embed_cost, 6),
    }


def set_stop_reason(reason: str) -> None:
    global _stop_reason
    with _stop_lock:
        if not _stop_reason:
            _stop_reason = reason
    _stop_event.set()


def get_stop_reason() -> str:
    with _stop_lock:
        return _stop_reason


def is_fatal_api_limit(error_message: str) -> bool:
    value = error_message.lower()
    keywords = [
        "resource_exhausted",
        "resource exhausted",
        "429",
        "rate limit",
        "rate_limit",
        "too many requests",
        "quota exceeded",
        "quota_exceeded",
        "insufficient quota",
        "insufficient credits",
        "insufficient credit",
        "credit balance",
        "billing account",
        "billing disabled",
        "payment required",
        "402",
    ]
    return any(keyword in value for keyword in keywords)


def is_transient_api_error(error_message: str) -> bool:
    value = error_message.lower()
    keywords = [
        "timeout",
        "deadline",
        "unavailable",
        "connection",
        "temporarily",
        "internal server error",
        "500",
        "502",
        "503",
        "504",
    ]
    return any(keyword in value for keyword in keywords)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract metadata and embed Instagram captions into a checkpointed MongoDB test collection."
    )
    parser.add_argument("--source", choices=["mongodb", "mysql"], default="mongodb")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--location-id", type=int, default=DEFAULT_LOCATION_ID)
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--reset-checkpoint", action="store_true")
    return parser.parse_args()


# Extraction prompt


EXTRACTION_PROMPT = """\
You are analyzing an Instagram post caption (it may include transcript-like text).
Return ONLY valid JSON, no markdown, no extra text.

CRITICAL RULE: ALL TEXT OUTPUT MUST BE IN ENGLISH. Tags, categories, and all
other text fields MUST be English only. Even if the caption is entirely Chinese,
Japanese, Korean, or any other language, your outputs must be in English.
This is non-negotiable. Exactly 10 tags are required.

If a proper noun or name contains non-English characters (e.g., "Brian在日本住"),
transliterate or translate it to English: "Brian在日本住" → "Brian In Japan".
Do NOT leave any non-English text in any output field. All 10 tags must be
composed entirely of A-Z, a-z, digits 0-9, and spaces only.

AMBIGUOUS TERMS RULE: Many place names, cultural terms, and landmarks share
the same name across different countries and languages (e.g., "東北" = NE China
vs Tohoku Japan; "Cambridge" = UK vs US; "Sydney" = Australia vs Canada; 
"Georgia" = US state vs country). Use the caption's language, grammar, 
colloquialisms, food references, and cultural context to determine the correct
interpretation. When a caption is written in Chinese (Cantonese/Mandarin) with
Chinese cultural references, prefer Chinese geographic interpretations. When
written in Japanese with Japanese context, prefer Japanese interpretations.
If the language and cultural signals are unclear, choose the most likely
global interpretation.

Task A: Generate exactly 10 topic tags in English.

Goal:
Produce high-level, stable THEMES for content discovery. Tags must be specific
enough to be useful, but not random words.

CRITICAL: These tags are used for SEMANTIC SEARCH FILTERING. Generic tags
like "Travel Update" make the filter useless. Specific tags like "Japan Road
Trip" or "Okinawa Beach" make search actually work.

Hard grounding rules (no hallucination):
- Every tag MUST be directly supported by the caption text.
- If the caption does not clearly support a tag, DO NOT output it.
- Do NOT invent names, roles, locations, dates, or events.
- Do NOT copy arbitrary phrases; prefer normalized themes.

Tag style rules:
- Exactly 10 tags.
- Each tag is 1 to 4 words.
- Use only letters A-Z/a-z, digits 0-9, and spaces.
- No emojis, no punctuation, no symbols, no hashtags, no non-Latin characters.
- Use Title Case for all tags.

STRICTLY BANNED filler tags (DO NOT USE):
General Topic, Lifestyle, Trending Topic, Pop Culture, Creator Content,
Daily Life, Social Media, Video Content, Entertainment, Travel Update,
Personal Thoughts, Vacation, Journey, Travel Experience, Travel Vlog,
Holiday, Travel Diary, Travel Lifestyle, Travel Planning, Travel Tips,
Daily Update, Life Update, Personal Growth, Life Reflection, Mindfulness,
Positive Thinking, Travel Inspiration, Travel Memories, Travel Community.

DO NOT use meta tags about the post format unless clearly central
(e.g., Vlog, Tutorial, Review, Behind The Scenes).

How to choose tags — DISCRIMINATION RULE:
Pick tags that would help DISTINGUISH this post from other similar posts.
"Travel" matches millions of posts. "Okinawa Road Trip" matches only this one.

1) First infer ONE main theme and 2 to 4 secondary themes from the caption.
2) ALWAYS include a LOCATION tag if a place is mentioned or clearly implied
   (e.g., Tokyo, Okinawa, Thailand, Hong Kong, Bali).
3) Prefer tags from these theme types when present:
   - Activity or domain: Scuba Diving, Hiking, Street Photography, Cafe Hopping
   - Specific place or region ONLY if explicitly stated: Tokyo, Okinawa, Seoul
   - Media or genre ONLY if explicit: Travel Vlog, Food Review, Kpop
   - Food or item ONLY if explicit: Ramen, Bubble Tea, Sushi
   - Occasion ONLY if explicit: Christmas, Cherry Blossom Season
   - Emotion or self-development ONLY if explicit and central: Solo Travel,
     Personal Challenge
4) If the caption is too short or vague, fall back to broader grounded tags:
   Daily Update, Personal Thoughts, Work Life, Family Moment, Food Moment,
   Music Clip, TV Show, Shopping Trip, City Walk, Pet Moment, Fitness Update
   — but still prefer specific location + activity combos over these.

Quality constraints:
- Tags must be distinct (no near-duplicates).
- Do not include Part One, Part Two, Episode Numbers, or similar sequencing.
- Do not include single letters or single generic words like Nice, Happy, Good.
- If a brand is mentioned, you may include a brand-related tag ONLY if it
  represents the content theme (e.g., Nike Running), otherwise omit.
- At least 3 of the 10 tags should be SPECIFIC enough to uniquely identify
  this post's main subject (e.g., "Okinawa Diving" not just "Ocean").

Task B: Extract brand names mentioned in the caption.
- Identify the formal names of brands mentioned (e.g., 'Nike', 'Disney',
  'Coca-Cola').
- These are natural language names, not handles.
- Output as a list of strings. Empty list if none.

Task C: Extract Instagram usernames that are mentioned in the caption and
separate BRANDS vs INFLUENCERS.
Act as a Social Media Data Miner. Your goal is to extract Instagram usernames
that are mentioned without the @ symbol. You must be extremely strict to avoid
extracting normal words, names, or titles.

Candidate extraction rules:
- Only consider tokens that are ALL LOWERCASE and contain '_' or '.'
  (e.g., 'elsie_lui', 'thegrand_hk').
- Also extract if two lowercase tokens appear together (cluster pattern),
  e.g., 'bakerybythegrand thegrand_hk'.
- Also extract lowercase tokens immediately after Chinese words like '同' or
  '感謝' if they look like usernames and are not dictionary words.

Exclusion rules (CRITICAL):
- If a word starts with a Capital Letter, it is a proper noun/name; DO NOT
  extract.
- Ignore event names and common nouns.
- Ignore single common first names.

Now classify each extracted username into one of two lists:
1) associated_brand: brand/company/shop/venue/product/service accounts.
2) associated_mention: influencer/creator/personal accounts.

Classification heuristics (use strict best-effort):
- Put into associated_brand if the username contains business/brand indicators
  such as: hk, hkg, official, shop, store, mall, hotel, restaurant, cafe, bar,
  dining, studio, salon, clinic, spa, beauty, skincare, makeup, cosmetics,
  fashion, jewelry, watch, travel, tours, airline, bank, insurance, comms, pr,
  agency, media, group, ltd, co, company, brand, boutique, bakery, kitchen,
  grill, izakaya, ramen, pizza, coffee, tea, dessert, sports, football, adidas,
  nike, puma, uniqlo, disney, hermes, dior, sephora, zeiss, owndays, bvlgari.
- Put into associated_brand if the caption context around it is
  promotional/brand-like: 'shop', 'link in bio', 'code', 'discount', 'book',
  'reservation', 'available at', 'now at', 'menu', 'treatment', 'package',
  'launch', 'drop'.
- Put into associated_mention if the username looks like a person/creator:
  contains a personal name pattern, or creator indicators such as: mua,
  makeupartist, artist, photographer, photo, videographer, editor, stylist,
  hair, nails, coach, trainer, dancer, actor, singer, model, dj, yoga.
- If uncertain, default to associated_mention unless there is a clear
  brand/business indicator.

Output requirements:
- Output extracted usernames WITHOUT leading '@'.
- Each entry must be ONE token with NO spaces.
- Allowed characters: letters a-z, digits 0-9, underscore _, dot ., and
  hyphen -.
- Deduplicate per list (case-insensitive dedupe OK, output first-seen form).
- If none, output empty list [].

Task D: Identify which words in the caption were originally hashtags.
- Output the hashtag words WITHOUT the # symbol.
- Only include words that were clearly used as hashtags in the original caption.
- If none, output empty list [].

Task E: Classify is_sponsorship.
is_sponsorship MUST be either 1 (true) or 0 (false).

Goal: In real Instagram captions, sponsorship is often IMPLIED. You must not be
overly conservative. Set is_sponsorship=1 when the caption is promoting a
brand/product/service and includes a brand reference.

Primary rule (most common case):
Set is_sponsorship=1 if associated_brand is NOT empty AND the caption contains
ANY promotional intent.

Promotional intent includes ANY of the following (treat as strong):
- Call to action: shop, order, buy, purchase, pre order, available now, drop,
  launch, link in bio, check out, tap, click, swipe, DM to order, dm me,
  inbox, whatsapp, book now, booking, appointment, reserve
- Price or offer: $, %, off, discount, promo, voucher, deal, sale, limited
  time, special offer, free delivery, code, use code, referral
- Product/service push: try this, must have, highly recommend, new product,
  new menu, new treatment, package, set, combo, best seller
- Stock/location: available at, now at, in store, online, website, hotline
- Brand shoutout patterns: thanks, thank you, shoutout followed by a
  brand/handle
- Partnership words: ad, sponsored, paid partnership, collab, collaboration,
  partnered with, ambassador, affiliate, gifted, PR, sent me

Secondary rule (when no associated_brand found):
Set is_sponsorship=1 if brand_name is NOT empty AND the caption contains ANY
of:
- explicit partnership words (ad, sponsored, paid partnership, collab,
  ambassador, affiliate, gifted, PR)
- a promo mechanic (code, discount, voucher, referral, link in bio)
- a clear call to action (shop, order, book now, DM to order)

Do NOT set is_sponsorship=1 only for:
- Pure personal opinion with no promotion
  (e.g., 'I love Nike shoes')
- Pure event attendance or generic tags with no selling/CTA

Important bias rule:
If there is ANY doubt and there is a brand reference (brand_name or
associated_brand) PLUS any promotional intent word, choose is_sponsorship=1.

Task F: Classify whether the caption contains specific, searchable content.
caption_is_specific MUST be either 1 (true) or 0 (false).

Set caption_is_specific=1 if the caption describes a specific:
- Activity, event, or experience (e.g., "Hiked Mount Fuji at sunrise")
- Place, product, or item (e.g., "Best ramen in Tokyo")
- Person, brand, or media (e.g., "Watching the new Dune movie")
- Opinion or review with substance (e.g., "This camera is amazing for vlogging")

Set caption_is_specific=0 if the caption is:
- A generic greeting or sign-off (e.g., "Good morning", "Hello everyone")
- Vague or cryptic (e.g., "Well...it just happened", "Life is good")
- Only emojis, single words, or filler (e.g., "❤️", "Thanks", "Happy")
- A generic quote or proverb without context
- Purely self-referential with no subject (e.g., "Just thinking", "Random thoughts")
- VERY SHORT text that doesn't clearly describe an activity, place, or thing
  (e.g., "Dating jap 🤗", "Japan 🤗", "Nice day", "Travel time")
  — even if it contains a location keyword like "Japan", the text is too
  short to infer meaningful searchable content.

Task G: Classify the post into exactly one category.
category MUST be exactly one of the following strings:
- BEAUTY_MAKEUP — makeup tutorial, skincare, cosmetics review, beauty product
- TRAVEL — travel diary, destination guide, trip planning, sightseeing
- FOOD — restaurant review, recipe, cooking, food photo, cafe
- FASHION — outfit, style, shopping haul, accessories, OOTD
- MUSIC — song, instrument, concert, music video, artist
- PERSONAL — personal update, life reflection, daily vlog, family
- SHOPPING — product promotion, deal, discount, haul, e-commerce
- EDUCATION — tutorial, how-to, tips, learning (non-beauty)
- NEWS — news, current events, information share
- SPORTS — fitness, workout, sports event, athlete
- PET — animal, pet, cat, dog
- OTHER — none of the above

Choose ONE category that best describes the post's PRIMARY content.
If a post is about a makeup tutorial, category is BEAUTY_MAKEUP even if travel is briefly mentioned.

Output schema exactly (ALL text values MUST be in English):
{"tags":["tag1","tag2",...,"tag10"],"brand_name":["brand1",...],"associated_brand":["handle1",...],"associated_mention":["handle1",...],"hashtags":["hashtag1",...],"is_sponsorship":0,"caption_is_specific":1,"category":"BEAUTY_MAKEUP"}

<caption>
{CAPTION_TEXT}
</caption>"""



# Helpers

def is_meaningful_caption(text: Optional[str]) -> bool:
    """Check whether a caption has enough text to embed."""
    if not text or not isinstance(text, str):
        return False
    stripped = text.strip()
    if len(stripped) < MIN_CAPTION_LENGTH:
        return False
    if not any(char.isalpha() for char in stripped):
        return False
    return True


def clean_text(text: str) -> str:
    """Clean caption text for embedding and display: normalize whitespace but keep paragraph structure."""
    text = text.strip()
    # Remove null bytes
    text = text.replace("\x00", "")
    # Normalize line endings
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    # Collapse 3+ consecutive newlines to 2 (keep paragraph separation)
    text = re.sub(r"\n{3,}", "\n\n", text)
    # Trim leading/trailing whitespace per line
    lines = [line.strip() for line in text.split("\n")]
    text = "\n".join(lines)
    return text.strip()


# Windows-1252 to Unicode mapping for bytes 0x80-0x9F
# These are the "printable" characters in the C1 control code range
_W1252_MAP = {
    0x20AC: 0x80,  # €
    0x201A: 0x82,  # ‚
    0x0192: 0x83,  # ƒ
    0x201E: 0x84,  # „
    0x2026: 0x85,  # …
    0x2020: 0x86,  # †
    0x2021: 0x87,  # ‡
    0x02C6: 0x88,  # ˆ
    0x2030: 0x89,  # ‰
    0x0160: 0x8A,  # Š
    0x2039: 0x8B,  # ‹
    0x0152: 0x8C,  # Œ
    0x017D: 0x8E,  # Ž
    0x2018: 0x91,  # '
    0x2019: 0x92,  # '
    0x201C: 0x93,  # "
    0x201D: 0x94,  # "
    0x2022: 0x95,  # •
    0x2013: 0x96,  # –
    0x2014: 0x97,  # —
    0x02DC: 0x98,  # ˜
    0x2122: 0x99,  # ™
    0x0161: 0x9A,  # š
    0x203A: 0x9B,  # ›
    0x0153: 0x9C,  # œ
    0x017E: 0x9E,  # ž
    0x0178: 0x9F,  # Ÿ
}


def fix_mojibake(text: str) -> str:
    """
    Fix double-encoded UTF-8 text (mojibake).

    Some caption data in MySQL was stored as: original Chinese → UTF-8 bytes →
    interpreted as Windows-1252 → stored as UTF-8 again.

    This reverses the corruption: garbled string → map chars to Windows-1252
    bytes → decode as UTF-8.

    Only transforms text that exhibits the mojibake pattern (contains chars
    in the Windows-1252 extended range). Pure ASCII/valid text passes through
    unchanged.
    """
    if not text:
        return text

    # Quick check: if no character has ord > 127, it's pure ASCII — no fix needed
    needs_fix = any(ord(ch) > 127 for ch in text)
    if not needs_fix:
        return text

    # Map each Unicode character to its Windows-1252 byte value
    recovered_bytes = bytearray()
    had_mapping = False

    for ch in text:
        code_point = ord(ch)
        if code_point < 0x80:
            # ASCII — passes through
            recovered_bytes.append(code_point)
        elif code_point in _W1252_MAP:
            # Windows-1252 extended character — map to its byte value
            recovered_bytes.append(_W1252_MAP[code_point])
            had_mapping = True
        elif code_point < 0x100:
            # Latin-1 range (0x80-0xFF), not in W1252 map — use as-is
            recovered_bytes.append(code_point)
            if code_point >= 0x80:
                had_mapping = True
        else:
            # Not mappable — keep the character as-is in UTF-8
            recovered_bytes.extend(ch.encode("utf-8"))

    if not had_mapping:
        # No Windows-1252 mapping was applied — text was valid UTF-8 all along
        return text

    try:
        return recovered_bytes.decode("utf-8")
    except UnicodeDecodeError:
        # If decoding fails, the text wasn't actually mojibake
        return text


def sleep_with_backoff(attempt: int) -> None:
    """Exponential backoff, max 60 seconds."""
    delay = min(60.0, (1.5 * (2 ** attempt)))
    time.sleep(delay)


def _ensure_10_tags(tags: List[str]) -> List[str]:
    """Filter to English-only tags and pad to exactly 10 if needed."""
    english = [t for t in tags if all(ord(c) < 128 for c in t)]
    if len(english) >= 10:
        return english[:10]
    # Pad with derived tags from remaining English tags
    if not english:
        return ["Content"] * 10
    padded = list(english)
    while len(padded) < 10:
        padded.append(f"Content {len(padded)}")
    return padded




def connect_mongodb() -> Tuple[MongoClient, Any, Any]:
    """Connect to MongoDB Atlas and return (client, database, source_collection)."""
    if not MONGO_URI:
        raise SystemExit("FATAL: MONGO_URI_ATLAS is not set in .env")
    client = MongoClient(MONGO_URI)
    database = client[MONGO_DATABASE]
    source_collection = database[MONGO_SOURCE_COLLECTION]
    return client, database, source_collection


def fetch_mongodb_post_ids(
    collection: Any,
    max_posts: int,
) -> List[Dict[str, Any]]:
    """
    Randomly sample documents from MongoDB ig-post collection.

    Uses a two-step approach for speed:
      1. $sample on _id only (fast — no full document scan)
      2. Fetch full documents by _id (indexed)

    Each batch gets truly random documents across the full 3.5M range.
    """
    # Step 1: get random _ids using $sample on projection only
    random_ids = []
    for doc in collection.aggregate(
        [{"$sample": {"size": max_posts}}, {"$project": {"_id": 1}}],
        allowDiskUse=True,
    ):
        random_ids.append(doc["_id"])

    if not random_ids:
        return []

    # Step 2: fetch full documents by _id
    records = []
    for doc in collection.find({"_id": {"$in": random_ids}}):
        record = {
            "mongo_id": doc["_id"],
            "user_id": str(doc.get("user_id", "")),
            "post_id": str(doc.get("post_id", "")),
            "locationId": doc.get("locationId"),
            "stats": doc.get("stats"),
        }
        records.append(record)

    print(f"[mongodb] Randomly sampled {len(records)} documents from {MONGO_SOURCE_COLLECTION}")
    return records





def connect_mysql() -> Any:
    """Create and return a MySQL connection."""
    import pymysql # type: ignore
    from pymysql.cursors import DictCursor # type: ignore

    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        charset="utf8mb4",
        autocommit=True,
        use_unicode=True,
        cursorclass=DictCursor,
        connect_timeout=12,
        read_timeout=90,
        write_timeout=90,
    )


def fetch_captions_from_mysql(
    post_pairs: List[Tuple[str, str]],
) -> Dict[Tuple[str, str], str]:
    """
    Fetch caption content from MySQL ig_post for the given (user_id, post_id) pairs.

    Returns a dict keyed by (user_id, post_id) → caption text.
    Only pairs with meaningful captions are included.
    """
    if not post_pairs:
        return {}

    connection = connect_mysql()
    caption_map: Dict[Tuple[str, str], str] = {}

    try:
        with connection.cursor() as cursor:
            for batch_start in range(0, len(post_pairs), 500):
                batch = post_pairs[batch_start:batch_start + 500]
                # Build a multi-row lookup query
                conditions = " OR ".join(
                    ["(ig_user_id = %s AND ig_post_id = %s)" for _ in batch]
                )
                query = f"""
                    SELECT CAST(ig_user_id AS CHAR) AS user_id,
                           CAST(ig_post_id AS CHAR) AS post_id,
                           content
                    FROM ig_post
                    WHERE {conditions}
                """
                params = []
                for user_id, post_id in batch:
                    params.extend([user_id, post_id])

                cursor.execute(query, tuple(params))
                for row in cursor.fetchall() or []:
                    content = (row.get("content") or "").strip()
                    if is_meaningful_caption(content):
                        key = (str(row["user_id"]), str(row["post_id"]))
                        caption_map[key] = clean_text(fix_mojibake(content))
    finally:
        connection.close()

    print(f"[mysql] Fetched {len(caption_map)} captions from ig_post")
    return caption_map


def fetch_posts_from_mysql_direct(
    location_id: int,
    max_posts: int,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """
    Read new post IDs and captions directly from MySQL (skip MongoDB lookup).

    Returns list of dicts with keys: user_id, post_id, caption.
    """
    query = """
        SELECT CAST(ig_user_id AS CHAR) AS user_id,
               CAST(ig_post_id AS CHAR) AS post_id,
               content
        FROM ig_post
        WHERE content IS NOT NULL
          AND LENGTH(TRIM(content)) > %s
          AND content REGEXP '[a-zA-Z]'
        ORDER BY postDate DESC
        LIMIT %s OFFSET %s
    """
    connection = connect_mysql()
    posts = []
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, (MIN_CAPTION_LENGTH, max_posts, offset))
            for row in cursor.fetchall() or []:
                content = (row.get("content") or "").strip()
                if is_meaningful_caption(content):
                    posts.append({
                        "user_id": str(row["user_id"]),
                        "post_id": str(row["post_id"]),
                        "caption": clean_text(fix_mojibake(content)),
                    })
    finally:
        connection.close()

    print(f"[mysql] Direct read: {len(posts)} posts from ig_post (location_id={location_id})")
    return posts





def extract_text_from_response(response: Any) -> str:
    """Extract text from a Gemini GenerateContent response."""
    if hasattr(response, "text") and response.text:
        return response.text.strip()
    if hasattr(response, "candidates") and response.candidates:
        candidate = response.candidates[0]
        if hasattr(candidate, "content") and candidate.content:
            parts = candidate.content.parts
            return "".join(
                part.text for part in parts if hasattr(part, "text") and part.text
            ).strip()
    return ""


def parse_extraction_response(raw_text: str) -> Optional[Dict[str, Any]]:
    """
    Parse the JSON response from Gemini into a structured dictionary.
    Handles markdown code fences.
    """
    if not raw_text:
        return None
    cleaned = raw_text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    cleaned = cleaned.strip()
    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict):
        return None
    return {
        "tags": parsed.get("tags", []) if isinstance(parsed.get("tags"), list) else [],
        "brand_name": (
            parsed.get("brand_name", [])
            if isinstance(parsed.get("brand_name"), list)
            else []
        ),
        "associated_brand": (
            parsed.get("associated_brand", [])
            if isinstance(parsed.get("associated_brand"), list)
            else []
        ),
        "associated_mention": (
            parsed.get("associated_mention", [])
            if isinstance(parsed.get("associated_mention"), list)
            else []
        ),
        "hashtags": (
            parsed.get("hashtags", [])
            if isinstance(parsed.get("hashtags"), list)
            else []
        ),
        "is_sponsorship": 1 if parsed.get("is_sponsorship") else 0,
        "caption_is_specific": 1 if parsed.get("caption_is_specific") else 0,
        "category": str(parsed.get("category", "OTHER")).strip().upper(),
    }


def empty_metadata() -> Dict[str, Any]:
    """Return an empty metadata structure for failed extractions."""
    return {
        "tags": [],
        "brand_name": [],
        "associated_brand": [],
        "associated_mention": [],
        "hashtags": [],
        "is_sponsorship": 0,
        "caption_is_specific": 0,
        "category": "OTHER",
    }




def extract_metadata(client: genai.Client, caption_text: str) -> Dict[str, Any]:
    prompt = EXTRACTION_PROMPT.replace("{CAPTION_TEXT}", caption_text)
    last_error = "metadata extraction failed"
    for attempt in range(MAX_RETRIES):
        if _stop_event.is_set():
            raise PipelineStop(get_stop_reason() or "pipeline stop requested")
        try:
            response = client.models.generate_content(
                model=GEMINI_EXTRACTION_MODEL,
                contents=[{"role": "user", "parts": [{"text": prompt}]}],
                config=genai_types.GenerateContentConfig(temperature=0.2),
            )
            raw_text = extract_text_from_response(response)
            usage = getattr(response, "usage_metadata", None)
            if usage:
                track_usage(
                    "extraction",
                    getattr(usage, "prompt_token_count", 0),
                    getattr(usage, "candidates_token_count", 0),
                )
            elif raw_text:
                track_usage("extraction", max(1, len(prompt) // 3), max(1, len(raw_text) // 4))
            parsed = parse_extraction_response(raw_text)
            if parsed is not None:
                return parsed
            last_error = "Gemini returned invalid extraction JSON"
        except PipelineStop:
            raise
        except Exception as error:
            message = str(error)
            if is_fatal_api_limit(message):
                reason = f"Gemini extraction stopped: {message[:500]}"
                set_stop_reason(reason)
                raise PipelineStop(reason) from error
            last_error = message[:500]
            if not is_transient_api_error(message) and attempt >= MAX_RETRIES - 1:
                break
        if attempt < MAX_RETRIES - 1:
            sleep_with_backoff(attempt)
    raise PostProcessingError(last_error)


def embed_text(client: genai.Client, text: str) -> List[float]:
    last_error = "embedding generation failed"
    for attempt in range(MAX_RETRIES):
        if _stop_event.is_set():
            raise PipelineStop(get_stop_reason() or "pipeline stop requested")
        try:
            response = client.models.embed_content(
                model=GEMINI_EMBEDDING_MODEL,
                contents=[text],
                config=genai_types.EmbedContentConfig(
                    task_type="SEMANTIC_SIMILARITY",
                    output_dimensionality=EMBED_DIM,
                ),
            )
            embeddings = getattr(response, "embeddings", None)
            if not embeddings or not isinstance(embeddings, list):
                raise RuntimeError("No embeddings returned")
            values = getattr(embeddings[0], "values", None)
            if not values or not isinstance(values, list):
                raise RuntimeError("Missing embedding values")
            usage = getattr(response, "usage_metadata", None)
            if usage:
                track_usage("embedding", getattr(usage, "prompt_token_count", 0))
            else:
                track_usage("embedding", max(1, len(text) // 3))
            return [float(value) for value in values]
        except PipelineStop:
            raise
        except Exception as error:
            message = str(error)
            if is_fatal_api_limit(message):
                reason = f"Gemini embedding stopped: {message[:500]}"
                set_stop_reason(reason)
                raise PipelineStop(reason) from error
            last_error = message[:500]
            if not is_transient_api_error(message) and attempt >= MAX_RETRIES - 1:
                break
        if attempt < MAX_RETRIES - 1:
            sleep_with_backoff(attempt)
    raise PostProcessingError(last_error)


def ensure_temp_collection_indexes(
    database: Any,
    source_collection: Any,
    temp_collection_name: str,
) -> None:
    """Copy all regular indexes from source collection to temp collection.

    Call once before processing. Atlas Search (vector) indexes must be created
    separately through the Atlas UI or API.
    """
    temp_collection = database[temp_collection_name]
    source_indexes = list(source_collection.list_indexes())
    for index in source_indexes:
        index_name = index.get("name", "")
        if index_name == "_id_":
            continue
        index_keys = list(index["key"].items())
        index_options = {}
        for option_key in ("unique", "sparse", "background", "partialFilterExpression",
                           "expireAfterSeconds", "hidden", "storageEngine", "weights",
                           "default_language", "language_override", "textIndexVersion",
                           "2dsphereIndexVersion", "bits", "min", "max", "bucketSize"):
            if option_key in index:
                index_options[option_key] = index[option_key]
        try:
            temp_collection.create_index(index_keys, **index_options)
        except Exception as index_error:
            print(f"  [mongodb] Index {index_name} skipped: {index_error}")
    try:
        temp_collection.create_index([("post_id", 1)], name="post_id_lookup")
    except Exception as index_error:
        print(f"  [mongodb] Index post_id_lookup skipped: {index_error}")
    print(f"[mongodb] Indexes ready on {temp_collection_name}")




def atomic_write_json(path: str, value: Any) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2, default=str)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, destination)


def atomic_write_bson_json(path: str, value: Any) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(json_util.dumps(value, ensure_ascii=False, indent=2))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, destination)


def load_plain_json(path: str, default: Any) -> Any:
    source = Path(path)
    if not source.exists():
        return default
    try:
        with source.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return default


def load_bson_json(path: str, default: Any) -> Any:
    source = Path(path)
    if not source.exists():
        return default
    try:
        return json_util.loads(source.read_text(encoding="utf-8"))
    except Exception:
        return default


def new_processed_state() -> Dict[str, Any]:
    return {
        "version": 1,
        "database": MONGO_DATABASE,
        "collection": MONGO_TEMP_COLLECTION,
        "updated_at": now_text(),
        "posts": {},
    }


def load_processed_state(reset: bool) -> Dict[str, Any]:
    if reset and Path(PROCESSED_POSTID_FILE).exists():
        Path(PROCESSED_POSTID_FILE).unlink()
    state = load_plain_json(PROCESSED_POSTID_FILE, new_processed_state())
    if not isinstance(state, dict) or not isinstance(state.get("posts"), dict):
        state = new_processed_state()
    if state.get("database") != MONGO_DATABASE or state.get("collection") != MONGO_TEMP_COLLECTION:
        state = new_processed_state()
    return state


def save_processed_state(state: Dict[str, Any]) -> None:
    state["updated_at"] = now_text()
    atomic_write_json(PROCESSED_POSTID_FILE, state)


def set_post_status(
    processed_state: Dict[str, Any],
    post: Dict[str, Any],
    status: str,
    reason: str,
    error: str = "",
) -> None:
    post_id = str(post.get("post_id", ""))
    if not post_id:
        return
    previous = processed_state["posts"].get(post_id, {})
    attempts = int(previous.get("attempts", 0) or 0)
    if status == "failed":
        attempts += 1
    elif attempts == 0:
        attempts = 1
    entry = {
        "post_id": post_id,
        "user_id": str(post.get("user_id", "")),
        "status": status,
        "reason": reason,
        "attempts": attempts,
        "updated_at": now_text(),
    }
    if error:
        entry["error"] = error[:1000]
    processed_state["posts"][post_id] = entry


def processed_counts(processed_state: Dict[str, Any]) -> Dict[str, int]:
    counts = {"success": 0, "failed": 0, "skip": 0}
    for value in processed_state.get("posts", {}).values():
        status = value.get("status")
        if status in counts:
            counts[status] += 1
    counts["total"] = counts["success"] + counts["failed"] + counts["skip"]
    return counts


def new_stage_state() -> Dict[str, Any]:
    return {
        "version": 1,
        "database": MONGO_DATABASE,
        "collection": MONGO_TEMP_COLLECTION,
        "updated_at": now_text(),
        "records": {},
    }


def load_stage_state() -> Dict[str, Any]:
    state = load_bson_json(TEMP_RECORDS_FILE, new_stage_state())
    if not isinstance(state, dict) or not isinstance(state.get("records"), dict):
        state = new_stage_state()
    if state.get("database") != MONGO_DATABASE or state.get("collection") != MONGO_TEMP_COLLECTION:
        state = new_stage_state()
    return state


def save_stage_state(state: Dict[str, Any]) -> None:
    state["updated_at"] = now_text()
    atomic_write_bson_json(TEMP_RECORDS_FILE, state)


def stage_record(stage_state: Dict[str, Any], record: Dict[str, Any]) -> None:
    stage_state["records"][str(record["post_id"])] = record
    save_stage_state(stage_state)


def new_checkpoint(limit: int) -> Dict[str, Any]:
    return {
        "version": 1,
        "database": MONGO_DATABASE,
        "source_collection": MONGO_SOURCE_COLLECTION,
        "target_collection": MONGO_TEMP_COLLECTION,
        "requested_limit": int(limit),
        "batch_number": 0,
        "mysql_offset": 0,
        "processing_time_seconds": 0.0,
        "usage": get_usage_snapshot(),
        "last_stop_reason": "",
        "stopped": False,
        "updated_at": now_text(),
    }


def load_checkpoint(limit: int, reset: bool) -> Dict[str, Any]:
    if reset and Path(CHECKPOINT_FILE).exists():
        Path(CHECKPOINT_FILE).unlink()
    state = load_plain_json(CHECKPOINT_FILE, new_checkpoint(limit))
    if not isinstance(state, dict):
        state = new_checkpoint(limit)
    if (
        state.get("database") != MONGO_DATABASE
        or state.get("source_collection") != MONGO_SOURCE_COLLECTION
        or state.get("target_collection") != MONGO_TEMP_COLLECTION
    ):
        state = new_checkpoint(limit)
    state["requested_limit"] = int(limit)
    return state


def save_checkpoint(
    checkpoint: Dict[str, Any],
    processed_state: Dict[str, Any],
    stage_state: Dict[str, Any],
    elapsed_seconds: float,
) -> None:
    counts = processed_counts(processed_state)
    checkpoint["total_processed"] = counts["total"]
    checkpoint["total_success"] = counts["success"]
    checkpoint["total_failed"] = counts["failed"]
    checkpoint["total_skipped"] = counts["skip"]
    checkpoint["pending_mongodb"] = len(stage_state.get("records", {}))
    checkpoint["processing_time_seconds"] = round(elapsed_seconds, 3)
    checkpoint["usage"] = get_usage_snapshot()
    checkpoint["cost_estimate_usd"] = get_usage_summary()
    checkpoint["updated_at"] = now_text()
    atomic_write_json(CHECKPOINT_FILE, checkpoint)


def build_summary(
    checkpoint: Dict[str, Any],
    processed_state: Dict[str, Any],
    stage_state: Dict[str, Any],
    elapsed_seconds: float,
    limit: int,
) -> Dict[str, Any]:
    counts = processed_counts(processed_state)
    usage = get_usage_summary()
    pending = len(stage_state.get("records", {}))
    handled = counts["total"] + pending
    rate = handled / elapsed_seconds if elapsed_seconds > 0 else 0.0
    remaining = max(0, int(limit) - handled)
    eta = remaining / rate if rate > 0 else None
    return {
        "requested_limit": int(limit),
        "total_processed": counts["total"],
        "success": counts["success"],
        "failed": counts["failed"],
        "skipped": counts["skip"],
        "pending_mongodb": pending,
        "handled_including_pending": handled,
        "remaining": remaining,
        "rate_posts_per_second": round(rate, 4),
        "eta_seconds": round(eta, 2) if eta is not None else None,
        "processing_time_seconds": round(elapsed_seconds, 2),
        "tokens": {
            "extraction_input": usage["extraction_input_tokens"],
            "extraction_output": usage["extraction_output_tokens"],
            "extraction_total": usage["extraction_total_tokens"],
            "embedding_input": usage["embedding_input_tokens"],
            "combined": usage["combined_tokens"],
        },
        "cost_usd": {
            "extraction": usage["extraction_cost_usd"],
            "embedding": usage["embedding_cost_usd"],
            "combined": usage["total_cost_usd"],
        },
        "batch_number": int(checkpoint.get("batch_number", 0) or 0),
        "stopped": bool(checkpoint.get("stopped", False)),
        "last_stop_reason": checkpoint.get("last_stop_reason", ""),
        "updated_at": now_text(),
    }


def eta_text(seconds: Optional[float]) -> str:
    if seconds is None:
        return "unknown"
    value = max(0, int(seconds))
    hours, remainder = divmod(value, 3600)
    minutes, seconds_value = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds_value:02d}"


def emit_summary(
    checkpoint: Dict[str, Any],
    processed_state: Dict[str, Any],
    stage_state: Dict[str, Any],
    elapsed_seconds: float,
    limit: int,
) -> Dict[str, Any]:
    summary = build_summary(checkpoint, processed_state, stage_state, elapsed_seconds, limit)
    atomic_write_json(REALTIME_SUMMARY_FILE, summary)
    tokens = summary["tokens"]
    costs = summary["cost_usd"]
    print(
        f"[summary] processed={summary['total_processed']}/{limit} "
        f"success={summary['success']} failed={summary['failed']} skipped={summary['skipped']} "
        f"pending={summary['pending_mongodb']} rate={summary['rate_posts_per_second']:.2f}/s "
        f"eta={eta_text(summary['eta_seconds'])} | "
        f"tokens extraction={tokens['extraction_input']}in/{tokens['extraction_output']}out "
        f"embedding={tokens['embedding_input']} combined={tokens['combined']} | "
        f"cost extraction=${costs['extraction']:.6f} embedding=${costs['embedding']:.6f} "
        f"combined=${costs['combined']:.6f}"
    )
    return summary


def existing_post_ids(collection: Any, post_ids: List[str]) -> Set[str]:
    clean_ids = sorted({str(value) for value in post_ids if str(value)})
    if not clean_ids:
        return set()
    query_values: List[Any] = list(clean_ids)
    for value in clean_ids:
        if value.isdigit():
            try:
                query_values.append(int(value))
            except Exception:
                pass
    found: Set[str] = set()
    for document in collection.find({"post_id": {"$in": query_values}}, {"post_id": 1}):
        found.add(str(document.get("post_id", "")))
    return found


def mongo_document(record: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "user_id": str(record.get("user_id", "")),
        "post_id": str(record.get("post_id", "")),
        "locationId": record.get("locationId"),
        "stats": record.get("stats"),
        "caption": record.get("caption", ""),
        "tags": _ensure_10_tags(record.get("tags") or []),
        "brand_name": record.get("brand_name", []),
        "associated_brand": record.get("associated_brand", []),
        "associated_mention": record.get("associated_mention", []),
        "hashtags": record.get("hashtags", []),
        "is_sponsorship": int(record.get("is_sponsorship", 0) or 0),
        "category": record.get("category", "OTHER"),
        "embedding": record.get("embedding", []),
        "embedding_model": record.get("embedding_model", GEMINI_EMBEDDING_MODEL),
        "embedding_updated_at": record.get("embedding_updated_at", time.time()),
        "gemini_extraction_model": record.get("gemini_extraction_model", GEMINI_EXTRACTION_MODEL),
        "gemini_processing_updated_at": record.get("gemini_processing_updated_at", time.time()),
    }


def insert_into_temp_collection(
    database: Any,
    temp_collection_name: str,
    records: List[Dict[str, Any]],
) -> Tuple[Set[str], Dict[str, str]]:
    if not records:
        return set(), {}
    collection = database[temp_collection_name]
    documents = [mongo_document(record) for record in records]
    operations = [
        UpdateOne(
            {"post_id": document["post_id"]},
            {"$set": document},
            upsert=True,
        )
        for document in documents
    ]
    bulk_error = ""
    try:
        collection.bulk_write(operations, ordered=False)
    except Exception as error:
        bulk_error = str(error)[:1000]
    lookup_error = ""
    try:
        successful = existing_post_ids(collection, [document["post_id"] for document in documents])
    except Exception as error:
        successful = set()
        lookup_error = str(error)[:1000]
    errors: Dict[str, str] = {}
    for document in documents:
        post_id = document["post_id"]
        if post_id in successful:
            continue
        try:
            result = collection.update_one(
                {"post_id": post_id},
                {"$set": document},
                upsert=True,
            )
            if result.acknowledged and (result.matched_count > 0 or result.upserted_id is not None):
                successful.add(post_id)
            else:
                errors[post_id] = "MongoDB did not acknowledge a matched or inserted document"
        except Exception as error:
            errors[post_id] = str(error)[:1000]
    if bulk_error or lookup_error:
        combined_error = " | ".join(value for value in (bulk_error, lookup_error) if value)
        for document in documents:
            post_id = document["post_id"]
            if post_id not in successful and post_id not in errors:
                errors[post_id] = combined_error
    return successful, errors


def flush_staged_records(
    mongo_database: Any,
    processed_state: Dict[str, Any],
    stage_state: Dict[str, Any],
    checkpoint: Dict[str, Any],
    elapsed_seconds: float,
    limit: int,
    force: bool,
) -> bool:
    while stage_state.get("records") and (force or len(stage_state["records"]) >= MONGO_FLUSH_SIZE):
        post_ids = list(stage_state["records"].keys())[:MONGO_FLUSH_SIZE]
        batch = [stage_state["records"][post_id] for post_id in post_ids]
        successful, errors = insert_into_temp_collection(
            mongo_database,
            MONGO_TEMP_COLLECTION,
            batch,
        )
        for post_id in successful:
            record = stage_state["records"].get(post_id)
            if record is not None:
                set_post_status(
                    processed_state,
                    record,
                    "success",
                    f"inserted_into_{MONGO_TEMP_COLLECTION}",
                )
        if successful:
            save_processed_state(processed_state)
            for post_id in successful:
                stage_state["records"].pop(post_id, None)
            save_stage_state(stage_state)
        if errors:
            reason = f"MongoDB flush failed for {len(errors)} staged posts"
            checkpoint["stopped"] = True
            checkpoint["last_stop_reason"] = reason
            save_checkpoint(checkpoint, processed_state, stage_state, elapsed_seconds)
            emit_summary(checkpoint, processed_state, stage_state, elapsed_seconds, limit)
            for post_id, error in errors.items():
                print(f"[mongodb] {post_id}: {error}")
            return False
        save_checkpoint(checkpoint, processed_state, stage_state, elapsed_seconds)
        emit_summary(checkpoint, processed_state, stage_state, elapsed_seconds, limit)
        if not force and len(stage_state["records"]) < MONGO_FLUSH_SIZE:
            break
    return True


def build_embedding_record(
    gemini_client: genai.Client,
    post: Dict[str, Any],
) -> Dict[str, Any]:
    if _stop_event.is_set():
        raise PipelineStop(get_stop_reason() or "pipeline stop requested")
    caption = post.get("caption", "")
    if not is_meaningful_caption(caption):
        return {"result": "skip", "reason": "caption_not_meaningful", "post": post}
    metadata = extract_metadata(gemini_client, caption)
    if not metadata.get("caption_is_specific", 0):
        return {"result": "skip", "reason": "caption_not_specific", "post": post}
    filler_tags = {
        "general topic", "lifestyle", "trending topic", "pop culture",
        "creator content", "daily life", "social media", "video content",
        "entertainment", "travel update", "personal thoughts", "vacation",
        "journey", "travel experience", "travel vlog", "holiday",
        "travel diary", "travel lifestyle", "travel planning", "travel tips",
        "daily update", "life update", "personal growth", "life reflection",
        "mindfulness", "positive thinking", "travel inspiration",
        "travel memories", "travel community", "work life", "family moment",
        "food moment", "music clip", "tv show", "shopping trip",
        "city walk", "pet moment", "fitness update", "road trip",
        "travel moment", "leisure time", "travel photography",
        "vacation mode", "summer vacation", "weekend getaway",
        "daily routine", "personal journey", "life experience",
        "travel destination",
    }
    raw_tags = metadata.get("tags", [])
    specific_tags = [
        tag
        for tag in raw_tags
        if isinstance(tag, str)
        and tag.lower() not in filler_tags
        and all(ord(character) < 128 for character in tag)
    ]
    category = metadata.get("category", "OTHER")
    if len(specific_tags) >= 5:
        parts = []
        if category and category != "OTHER":
            parts.append(f"Category: {category}")
        parts.append(caption)
        parts.append(f"Tags: {', '.join(raw_tags)}")
        embedding_input = "\n\n".join(parts)
    else:
        embedding_input = caption
    embedding_vector = embed_text(gemini_client, embedding_input)
    embedding_updated_at = time.time()
    record = {
        "user_id": str(post.get("user_id", "")),
        "post_id": str(post.get("post_id", "")),
        "locationId": post.get("locationId"),
        "stats": post.get("stats"),
        "caption": caption,
        "tags": _ensure_10_tags(metadata.get("tags", [])),
        "brand_name": [
            brand
            for brand in metadata.get("brand_name", [])
            if isinstance(brand, str) and all(ord(character) < 128 for character in brand)
        ],
        "associated_brand": metadata.get("associated_brand", []),
        "associated_mention": metadata.get("associated_mention", []),
        "hashtags": metadata.get("hashtags", []),
        "is_sponsorship": int(metadata.get("is_sponsorship", 0) or 0),
        "category": category,
        "embedding": embedding_vector,
        "embedding_model": GEMINI_EMBEDDING_MODEL,
        "embedding_updated_at": embedding_updated_at,
        "gemini_extraction_model": GEMINI_EXTRACTION_MODEL,
        "gemini_processing_updated_at": time.time(),
    }
    return {"result": "generated", "record": record, "post": post}


def process_post_batch(
    gemini_client: genai.Client,
    mongo_database: Any,
    posts: List[Dict[str, Any]],
    workers: int,
    processed_state: Dict[str, Any],
    stage_state: Dict[str, Any],
    checkpoint: Dict[str, Any],
    elapsed_provider: Any,
    limit: int,
) -> Dict[str, Any]:
    result_counts = {"generated": 0, "failed": 0, "skip": 0, "stopped": 0, "records": []}
    if not posts:
        return result_counts
    executor = ThreadPoolExecutor(max_workers=max(1, int(workers)))
    future_to_post = {
        executor.submit(build_embedding_record, gemini_client, post): post
        for post in posts
    }
    try:
        for future in as_completed(future_to_post):
            post = future_to_post[future]
            if future.cancelled():
                result_counts["stopped"] += 1
                continue
            try:
                outcome = future.result()
                if outcome["result"] == "skip":
                    set_post_status(processed_state, post, "skip", outcome["reason"])
                    save_processed_state(processed_state)
                    result_counts["skip"] += 1
                elif outcome["result"] == "generated":
                    stage_record(stage_state, outcome["record"])
                    result_counts["records"].append(outcome["record"])
                    result_counts["generated"] += 1
                    if len(stage_state["records"]) >= MONGO_FLUSH_SIZE:
                        if not flush_staged_records(
                            mongo_database,
                            processed_state,
                            stage_state,
                            checkpoint,
                            elapsed_provider(),
                            limit,
                            False,
                        ):
                            set_stop_reason(checkpoint.get("last_stop_reason", "MongoDB flush failed"))
                else:
                    raise PostProcessingError("Unknown processing result")
            except PipelineStop as error:
                set_stop_reason(str(error))
                result_counts["stopped"] += 1
            except Exception as error:
                set_post_status(
                    processed_state,
                    post,
                    "failed",
                    "processing_failed",
                    str(error),
                )
                save_processed_state(processed_state)
                result_counts["failed"] += 1
            checkpoint["stopped"] = _stop_event.is_set()
            checkpoint["last_stop_reason"] = get_stop_reason()
            save_checkpoint(
                checkpoint,
                processed_state,
                stage_state,
                elapsed_provider(),
            )
            emit_summary(
                checkpoint,
                processed_state,
                stage_state,
                elapsed_provider(),
                limit,
            )
            if _stop_event.is_set():
                for pending_future in future_to_post:
                    if not pending_future.done():
                        pending_future.cancel()
    finally:
        executor.shutdown(wait=True, cancel_futures=True)
    return result_counts


def save_local_backup(records: List[Dict[str, Any]], summary: Dict[str, Any]) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    atomic_write_bson_json(
        POSTS_BACKUP_FILE,
        {"metadata": summary, "posts": records},
    )


def process_posts() -> None:
    args = parse_arguments()
    max_posts = max(0, int(args.limit))
    batch_size = max(1, int(args.batch_size))
    workers = max(1, int(args.workers))
    reset = bool(args.reset_checkpoint)
    if reset:
        for path in (PROCESSED_POSTID_FILE, CHECKPOINT_FILE, REALTIME_SUMMARY_FILE):
            if Path(path).exists():
                Path(path).unlink()
    processed_state = load_processed_state(False)
    stage_state = load_stage_state()
    save_processed_state(processed_state)
    save_stage_state(stage_state)
    checkpoint = load_checkpoint(max_posts, False)
    restore_usage(checkpoint.get("usage", {}))
    checkpoint["stopped"] = False
    checkpoint["last_stop_reason"] = ""
    session_start = time.time()
    previous_elapsed = float(checkpoint.get("processing_time_seconds", 0.0) or 0.0)

    def elapsed_seconds() -> float:
        return previous_elapsed + (time.time() - session_start)

    print("=" * 70)
    print("Embedding Test Pipeline")
    print(f"Source: {args.source}")
    print(f"Post limit: {max_posts}")
    print(f"Batch size: {batch_size}")
    print(f"Workers: {workers}")
    print(f"Mongo source: {MONGO_SOURCE_COLLECTION}")
    print(f"Mongo destination: {MONGO_TEMP_COLLECTION}")
    print(f"Processed status: {PROCESSED_POSTID_FILE}")
    print(f"Temporary records: {TEMP_RECORDS_FILE}")
    print(f"Checkpoint: {CHECKPOINT_FILE}")
    print(f"Flush size: {MONGO_FLUSH_SIZE}")
    print("=" * 70)

    if not GEMINI_API_KEY:
        checkpoint["stopped"] = True
        checkpoint["last_stop_reason"] = "GEMINI_API_KEY not set in .env"
        save_checkpoint(checkpoint, processed_state, stage_state, elapsed_seconds())
        raise SystemExit("FATAL: GEMINI_API_KEY not set in .env")

    gemini_client = genai.Client(api_key=GEMINI_API_KEY)
    mongo_client, mongo_database, mongo_source_collection = connect_mongodb()
    destination_collection = mongo_database[MONGO_TEMP_COLLECTION]
    session_records: List[Dict[str, Any]] = []
    seen_session: Set[str] = set()
    empty_batches = 0

    try:
        ensure_temp_collection_indexes(
            mongo_database,
            mongo_source_collection,
            MONGO_TEMP_COLLECTION,
        )
        save_checkpoint(checkpoint, processed_state, stage_state, elapsed_seconds())
        emit_summary(checkpoint, processed_state, stage_state, elapsed_seconds(), max_posts)

        if stage_state.get("records"):
            print(f"[resume] Flushing {len(stage_state['records'])} staged records before new Gemini calls")
            if not flush_staged_records(
                mongo_database,
                processed_state,
                stage_state,
                checkpoint,
                elapsed_seconds(),
                max_posts,
                True,
            ):
                return

        while processed_counts(processed_state)["total"] < max_posts and not _stop_event.is_set():
            checkpoint["batch_number"] = int(checkpoint.get("batch_number", 0) or 0) + 1
            remaining = max_posts - processed_counts(processed_state)["total"]
            desired = min(batch_size, remaining)
            candidate_take = max(desired, min(desired * 3, batch_size * 3))
            print(f"\n[batch {checkpoint['batch_number']}] target={desired} candidates={candidate_take}")

            if args.source == "mongodb":
                candidates = fetch_mongodb_post_ids(mongo_source_collection, candidate_take)
                pairs = [(item["user_id"], item["post_id"]) for item in candidates]
                captions = fetch_captions_from_mysql(pairs)
                posts = []
                for item in candidates:
                    key = (item["user_id"], item["post_id"])
                    caption = captions.get(key)
                    if caption:
                        item["caption"] = caption
                        posts.append(item)
            else:
                offset = int(checkpoint.get("mysql_offset", 0) or 0)
                posts = fetch_posts_from_mysql_direct(
                    int(args.location_id),
                    candidate_take,
                    offset,
                )
                checkpoint["mysql_offset"] = offset + len(posts)
                for post in posts:
                    mongo_document_value = mongo_source_collection.find_one(
                        {"user_id": post["user_id"], "post_id": post["post_id"]},
                        {"_id": 1, "locationId": 1, "stats": 1},
                    )
                    if mongo_document_value:
                        post["mongo_id"] = mongo_document_value.get("_id")
                        post["locationId"] = mongo_document_value.get("locationId")
                        post["stats"] = mongo_document_value.get("stats")

            terminal_ids = set(processed_state.get("posts", {}).keys())
            staged_ids = set(stage_state.get("records", {}).keys())
            filtered: List[Dict[str, Any]] = []
            for post in posts:
                post_id = str(post.get("post_id", ""))
                if not post_id or post_id in terminal_ids or post_id in staged_ids or post_id in seen_session:
                    continue
                seen_session.add(post_id)
                filtered.append(post)
                if len(filtered) >= desired:
                    break

            if not filtered:
                empty_batches += 1
                save_checkpoint(checkpoint, processed_state, stage_state, elapsed_seconds())
                emit_summary(checkpoint, processed_state, stage_state, elapsed_seconds(), max_posts)
                if empty_batches >= 10:
                    checkpoint["stopped"] = True
                    checkpoint["last_stop_reason"] = "Ten consecutive batches contained no new processable posts"
                    break
                continue

            existing = existing_post_ids(
                destination_collection,
                [str(post["post_id"]) for post in filtered],
            )
            processable: List[Dict[str, Any]] = []
            for post in filtered:
                post_id = str(post["post_id"])
                if post_id in existing:
                    set_post_status(
                        processed_state,
                        post,
                        "skip",
                        f"already_exists_in_{MONGO_TEMP_COLLECTION}",
                    )
                else:
                    processable.append(post)
            if existing:
                save_processed_state(processed_state)
                save_checkpoint(checkpoint, processed_state, stage_state, elapsed_seconds())
                emit_summary(checkpoint, processed_state, stage_state, elapsed_seconds(), max_posts)

            if processable:
                batch_result = process_post_batch(
                    gemini_client,
                    mongo_database,
                    processable,
                    workers,
                    processed_state,
                    stage_state,
                    checkpoint,
                    elapsed_seconds,
                    max_posts,
                )
                session_records.extend(batch_result["records"])

            if _stop_event.is_set():
                checkpoint["stopped"] = True
                checkpoint["last_stop_reason"] = get_stop_reason()
                break

            if stage_state.get("records"):
                if not flush_staged_records(
                    mongo_database,
                    processed_state,
                    stage_state,
                    checkpoint,
                    elapsed_seconds(),
                    max_posts,
                    True,
                ):
                    break

            empty_batches = 0 if processable or existing else empty_batches + 1

        if stage_state.get("records") and not checkpoint.get("last_stop_reason", "").startswith("MongoDB flush failed"):
            flush_staged_records(
                mongo_database,
                processed_state,
                stage_state,
                checkpoint,
                elapsed_seconds(),
                max_posts,
                True,
            )
    except KeyboardInterrupt:
        set_stop_reason("Interrupted by user")
        checkpoint["stopped"] = True
        checkpoint["last_stop_reason"] = get_stop_reason()
        print("\nInterrupted. Checkpoint and staged records were preserved.")
    finally:
        if _stop_event.is_set():
            checkpoint["stopped"] = True
            checkpoint["last_stop_reason"] = get_stop_reason()
        final_summary = emit_summary(
            checkpoint,
            processed_state,
            stage_state,
            elapsed_seconds(),
            max_posts,
        )
        save_checkpoint(checkpoint, processed_state, stage_state, elapsed_seconds())
        save_local_backup(session_records, final_summary)
        mongo_client.close()

    print("\n" + "=" * 70)
    print("DONE")
    print(f"Processed: {final_summary['total_processed']}/{max_posts}")
    print(f"Success: {final_summary['success']}")
    print(f"Failed: {final_summary['failed']}")
    print(f"Skipped: {final_summary['skipped']}")
    print(f"Pending MongoDB: {final_summary['pending_mongodb']}")
    print(f"Elapsed: {final_summary['processing_time_seconds']:.1f}s")
    print(f"Stop reason: {checkpoint.get('last_stop_reason') or 'completed'}")
    print(f"Estimated combined cost: ${final_summary['cost_usd']['combined']:.6f}")
    print("=" * 70)


if __name__ == "__main__":
    try:
        process_posts()
    except Exception as error:
        print(f"\nFatal: {error}")
        sys.exit(1)