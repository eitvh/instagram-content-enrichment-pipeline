import sys
import time
import json
import re
import argparse
import settings
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Tuple
from pymongo import MongoClient, UpdateOne
from google import genai
from google.genai import types as genai_types
from prompt import EXTRACTION_PROMPT
from settings import CHECKPOINT_FILE, DEFAULT_BATCH_SIZE, DEFAULT_LIMIT, DEFAULT_LOCATION_ID, EMBEDDING_INPUT_PRICE, EMBED_DIM, EXTRACTION_INPUT_PRICE, EXTRACTION_OUTPUT_PRICE, GEMINI_API_KEY, GEMINI_EMBEDDING_MODEL, GEMINI_EXTRACTION_MODEL, MAX_RETRIES, MAX_WORKERS, MIN_CAPTION_LENGTH, MONGO_DATABASE, MONGO_SOURCE_COLLECTION, MONGO_TEMP_COLLECTION, MONGO_URI, MYSQL_DATABASE, MYSQL_HOST, MYSQL_PASSWORD, MYSQL_PORT, MYSQL_USER, OUTPUT_DIR, POSTS_BACKUP_FILE, PROCESSING_VERSION, SUMMARY_OUTPUT_FILE
from pymongo.errors import ExecutionTimeout
from settings import MONGO_QUERY_TIMEOUT_MS, MONGO_QUERY_ATTEMPTS
_usage_lock = threading.Lock()
_usage = {'extraction_input_tokens': 0, 'extraction_output_tokens': 0, 'embedding_input_tokens': 0}

def track_usage(category: str, input_tokens: int, output_tokens: int=0) -> None:
    with _usage_lock:
        if input_tokens:
            _usage[f'{category}_input_tokens'] = _usage.get(f'{category}_input_tokens', 0) + input_tokens
        if output_tokens:
            _usage[f'{category}_output_tokens'] = _usage.get(f'{category}_output_tokens', 0) + output_tokens

def get_usage_summary() -> Dict[str, Any]:
    with _usage_lock:
        extract_in = _usage.get('extraction_input_tokens', 0)
        extract_out = _usage.get('extraction_output_tokens', 0)
        embed_in = _usage.get('embedding_input_tokens', 0)
    extract_cost = extract_in / 1000000 * EXTRACTION_INPUT_PRICE + extract_out / 1000000 * EXTRACTION_OUTPUT_PRICE
    embed_cost = embed_in / 1000000 * EMBEDDING_INPUT_PRICE
    return {'extraction_model': GEMINI_EXTRACTION_MODEL, 'extraction_input_tokens': extract_in, 'extraction_output_tokens': extract_out, 'extraction_cost_usd': round(extract_cost, 6), 'embedding_model': GEMINI_EMBEDDING_MODEL, 'embedding_input_tokens': embed_in, 'embedding_cost_usd': round(embed_cost, 6), 'total_cost_usd': round(extract_cost + embed_cost, 6)}

def get_usage_snapshot() -> Dict[str, int]:
    with _usage_lock:
        return {'extraction_input_tokens': int(_usage.get('extraction_input_tokens', 0)), 'extraction_output_tokens': int(_usage.get('extraction_output_tokens', 0)), 'embedding_input_tokens': int(_usage.get('embedding_input_tokens', 0))}

def restore_usage(values: Dict[str, Any]) -> None:
    with _usage_lock:
        _usage['extraction_input_tokens'] = int(values.get('extraction_input_tokens', 0) or 0)
        _usage['extraction_output_tokens'] = int(values.get('extraction_output_tokens', 0) or 0)
        _usage['embedding_input_tokens'] = int(values.get('embedding_input_tokens', 0) or 0)

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Extract metadata and embed Instagram post captions, updating MongoDB ig-post documents.')
    parser.add_argument('--source', choices=['mongodb'], default='mongodb', help='Data source for post IDs and captions (default: MongoDB)')
    parser.add_argument('--limit', type=int, default=DEFAULT_LIMIT, help=f'Maximum total posts to process across checkpoint resumes (default: {DEFAULT_LIMIT})')
    parser.add_argument('--batch-size', type=int, default=DEFAULT_BATCH_SIZE, help=f'Posts per batch when loading from MongoDB (default: {DEFAULT_BATCH_SIZE}). Total limit is split into batches to avoid long loading times.')
    countries = parser.add_mutually_exclusive_group()
    countries.add_argument('--location-id', type=int, choices=[1, 3, 4], default=DEFAULT_LOCATION_ID, help='Country: 1=HK, 3=MY, 4=SG; defaults to DEFAULT_LOCATION_ID')
    countries.add_argument('--all-countries', action='store_true', help='Process MY, HK, then SG with separate collections, checkpoints, and limits')
    parser.add_argument('--workers', type=int, default=MAX_WORKERS, help=f'Number of concurrent workers (default: {MAX_WORKERS})')
    parser.add_argument('--skip-existing', action='store_true', help='Skip posts already completed for the current extraction and embedding models')
    parser.add_argument('--reset-checkpoint', action='store_true', help='Reset checkpoint counters before processing')
    args = parser.parse_args()
    if args.limit < 0:
        parser.error('--limit must be >= 0 (0 processes no posts)')
    if args.batch_size < 1 or args.workers < 1:
        parser.error('--batch-size and --workers must be >= 1')
    return args

def is_meaningful_caption(text: Optional[str]) -> bool:
    if not text or not isinstance(text, str):
        return False
    stripped = text.strip()
    if len(stripped) < MIN_CAPTION_LENGTH:
        return False
    if not any((char.isalpha() for char in stripped)):
        return False
    return True

def clean_text(text: str) -> str:
    text = text.strip()
    text = text.replace('\x00', '')
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    text = re.sub('\\n{3,}', '\n\n', text)
    lines = [line.strip() for line in text.split('\n')]
    text = '\n'.join(lines)
    return text.strip()
_W1252_MAP = {8364: 128, 8218: 130, 402: 131, 8222: 132, 8230: 133, 8224: 134, 8225: 135, 710: 136, 8240: 137, 352: 138, 8249: 139, 338: 140, 381: 142, 8216: 145, 8217: 146, 8220: 147, 8221: 148, 8226: 149, 8211: 150, 8212: 151, 732: 152, 8482: 153, 353: 154, 8250: 155, 339: 156, 382: 158, 376: 159}

def fix_mojibake(text: str) -> str:
    if not text:
        return text
    needs_fix = any((ord(ch) > 127 for ch in text))
    if not needs_fix:
        return text
    recovered_bytes = bytearray()
    had_mapping = False
    for ch in text:
        code_point = ord(ch)
        if code_point < 128:
            recovered_bytes.append(code_point)
        elif code_point in _W1252_MAP:
            recovered_bytes.append(_W1252_MAP[code_point])
            had_mapping = True
        elif code_point < 256:
            recovered_bytes.append(code_point)
            if code_point >= 128:
                had_mapping = True
        else:
            recovered_bytes.extend(ch.encode('utf-8'))
    if not had_mapping:
        return text
    try:
        return recovered_bytes.decode('utf-8')
    except UnicodeDecodeError:
        return text

def sleep_with_backoff(attempt: int) -> None:
    delay = min(60.0, 1.5 * 2 ** attempt)
    time.sleep(delay)

def _ensure_10_tags(tags: List[str]) -> List[str]:
    english = [t for t in tags if all((ord(c) < 128 for c in t))]
    if len(english) >= 10:
        return english[:10]
    if not english:
        return ['Content'] * 10
    padded = list(english)
    while len(padded) < 10:
        padded.append(f'Content {len(padded)}')
    return padded

def is_retryable_error(error_message: str) -> bool:
    message_lower = error_message.lower()
    keywords = ['resource_exhausted', '429', 'quota', 'rate', 'too many requests', 'timeout', 'deadline', 'unavailable', 'connection', 'temporarily']
    return any((keyword in message_lower for keyword in keywords))

def connect_mongodb() -> Tuple[MongoClient, Any, Any]:
    if not MONGO_URI:
        raise SystemExit('FATAL: MONGO_URI_ATLAS_MYHKSG is not set in .env')
    client = MongoClient(MONGO_URI)
    client.admin.command('ping')
    database = client[MONGO_DATABASE]
    source_collection = database[MONGO_SOURCE_COLLECTION]
    return (client, database, source_collection)

def fetch_mongodb_post_ids(collection: Any, max_posts: int, location_id: int) -> List[Dict[str, Any]]:
    requested_limit = max(0, int(max_posts))
    requested_location = int(location_id)
    if requested_limit == 0:
        return []
    conditions: List[Dict[str, Any]] = [{'caption': {'$type': 'string', '$ne': ''}}, {'$or': [{'gemini_extraction_model': {'$exists': False}}, {'gemini_extraction_model': {'$ne': GEMINI_EXTRACTION_MODEL}}, {'gemini_processing_updated_at': {'$exists': False}}, {'$and': [{'embedding.0': {'$exists': True}}, {'embedding_model': {'$ne': GEMINI_EMBEDDING_MODEL}}]}]}]
    if requested_location > 0:
        conditions.append({'locationId': {'$in': [requested_location, str(requested_location)]}})
    processing_filter: Dict[str, Any] = {'$and': conditions}
    projection = {'_id': 1, 'user_id': 1, 'post_id': 1, 'locationId': 1, 'stats': 1, 'caption': 1}
    print(f"[mongodb] Fetching up to {requested_limit} posts for locationId={(requested_location if requested_location > 0 else 'all')}...")

    for attempt in range(1, MONGO_QUERY_ATTEMPTS + 1):
        cursor = None
        try:
            # Allow the query planner to choose among available indexes.
            cursor = collection.find(processing_filter, projection)
            cursor = cursor.limit(requested_limit).max_time_ms(MONGO_QUERY_TIMEOUT_MS)
            # A failed cursor may have yielded partial results. Only return a
            # complete fetch; retrying must not advance processing/checkpoints.
            documents = list(cursor)
            break
        except ExecutionTimeout as error:
            if attempt == MONGO_QUERY_ATTEMPTS:
                raise RuntimeError(
                    f'MongoDB query exceeded {MONGO_QUERY_TIMEOUT_MS} ms '
                    f'for locationId={requested_location} after {attempt} attempts'
                ) from error
            delay = min(30, 2 ** attempt)
            print(
                f'[mongodb] Query timed out for locationId={requested_location} '
                f'(attempt {attempt}/{MONGO_QUERY_ATTEMPTS}); retrying in {delay}s',
                flush=True,
            )
        finally:
            if cursor is not None:
                cursor.close()
        time.sleep(delay)
    records: List[Dict[str, Any]] = []
    for doc in documents:
        raw_caption = doc.get('caption', '')
        caption = clean_text(fix_mojibake(raw_caption)) if isinstance(raw_caption, str) else ''
        if not caption:
            continue
        actual_location = doc.get('locationId')
        if requested_location > 0:
            try:
                if int(actual_location) != requested_location:
                    continue
            except (TypeError, ValueError):
                continue
        records.append({'mongo_id': doc.get('_id'), 'user_id': str(doc.get('user_id', '')), 'post_id': str(doc.get('post_id', '')), 'locationId': requested_location if requested_location > 0 else actual_location, 'stats': doc.get('stats'), 'caption': caption})
    print(f"[mongodb] Fetched {len(records)} documents from {MONGO_SOURCE_COLLECTION} for locationId={(requested_location if requested_location > 0 else 'all')}")
    return records

def connect_mysql() -> Any:
    import pymysql
    from pymysql.cursors import DictCursor
    return pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DATABASE, charset='utf8mb4', autocommit=True, use_unicode=True, cursorclass=DictCursor, connect_timeout=12, read_timeout=90, write_timeout=90)

def fetch_captions_from_mysql(post_pairs: List[Tuple[str, str]]) -> Dict[Tuple[str, str], str]:
    if not post_pairs:
        return {}
    connection = connect_mysql()
    caption_map: Dict[Tuple[str, str], str] = {}
    try:
        with connection.cursor() as cursor:
            for batch_start in range(0, len(post_pairs), 500):
                batch = post_pairs[batch_start:batch_start + 500]
                conditions = ' OR '.join(['(ig_user_id = %s AND ig_post_id = %s)' for _ in batch])
                query = f'\n                    SELECT CAST(ig_user_id AS CHAR) AS user_id,\n                           CAST(ig_post_id AS CHAR) AS post_id,\n                           content\n                    FROM ig_post\n                    WHERE {conditions}\n                '
                params = []
                for user_id, post_id in batch:
                    params.extend([user_id, post_id])
                cursor.execute(query, tuple(params))
                for row in cursor.fetchall() or []:
                    content = (row.get('content') or '').strip()
                    if is_meaningful_caption(content):
                        key = (str(row['user_id']), str(row['post_id']))
                        caption_map[key] = clean_text(fix_mojibake(content))
    finally:
        connection.close()
    print(f'[mysql] Fetched {len(caption_map)} captions from ig_post')
    return caption_map

def fetch_posts_from_mysql_direct(location_id: int, max_posts: int) -> List[Dict[str, Any]]:
    query = "\n        SELECT CAST(ig_user_id AS CHAR) AS user_id,\n               CAST(ig_post_id AS CHAR) AS post_id,\n               content\n        FROM ig_post\n        WHERE content IS NOT NULL\n          AND LENGTH(TRIM(content)) > %s\n          AND content REGEXP '[a-zA-Z]'\n        ORDER BY postDate DESC\n        LIMIT %s\n    "
    connection = connect_mysql()
    posts = []
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, (MIN_CAPTION_LENGTH, max_posts))
            for row in cursor.fetchall() or []:
                content = (row.get('content') or '').strip()
                if is_meaningful_caption(content):
                    posts.append({'user_id': str(row['user_id']), 'post_id': str(row['post_id']), 'caption': clean_text(fix_mojibake(content))})
    finally:
        connection.close()
    print(f'[mysql] Direct read: {len(posts)} posts from ig_post (location_id={location_id})')
    return posts

def extract_text_from_response(response: Any) -> str:
    if hasattr(response, 'text') and response.text:
        return response.text.strip()
    if hasattr(response, 'candidates') and response.candidates:
        candidate = response.candidates[0]
        if hasattr(candidate, 'content') and candidate.content:
            parts = candidate.content.parts
            return ''.join((part.text for part in parts if hasattr(part, 'text') and part.text)).strip()
    return ''

def parse_extraction_response(raw_text: str) -> Optional[Dict[str, Any]]:
    if not raw_text:
        return None
    cleaned = raw_text.strip()
    if cleaned.startswith('```'):
        cleaned = re.sub('^```(?:json)?\\s*', '', cleaned)
        cleaned = re.sub('\\s*```$', '', cleaned)
    cleaned = cleaned.strip()
    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict):
        return None
    return {'tags': parsed.get('tags', []) if isinstance(parsed.get('tags'), list) else [], 'brand_name': parsed.get('brand_name', []) if isinstance(parsed.get('brand_name'), list) else [], 'associated_brand': parsed.get('associated_brand', []) if isinstance(parsed.get('associated_brand'), list) else [], 'associated_mention': parsed.get('associated_mention', []) if isinstance(parsed.get('associated_mention'), list) else [], 'hashtags': parsed.get('hashtags', []) if isinstance(parsed.get('hashtags'), list) else [], 'is_sponsorship': 1 if parsed.get('is_sponsorship') else 0, 'caption_is_specific': 1 if parsed.get('caption_is_specific') else 0, 'category': str(parsed.get('category', 'OTHER')).strip().upper()}

def empty_metadata() -> Dict[str, Any]:
    return {'tags': [], 'brand_name': [], 'associated_brand': [], 'associated_mention': [], 'hashtags': [], 'is_sponsorship': 0, 'caption_is_specific': 0, 'category': 'OTHER'}

def extract_metadata(client: genai.Client, caption_text: str) -> Dict[str, Any]:
    prompt = EXTRACTION_PROMPT.replace('{CAPTION_TEXT}', caption_text)
    for attempt in range(MAX_RETRIES):
        try:
            response = client.models.generate_content(model=GEMINI_EXTRACTION_MODEL, contents=[{'role': 'user', 'parts': [{'text': prompt}]}], config=genai_types.GenerateContentConfig(temperature=0.2))
            raw_text = extract_text_from_response(response)
            if raw_text:
                usage = getattr(response, 'usage_metadata', None)
                if usage:
                    track_usage('extraction', getattr(usage, 'prompt_token_count', 0), getattr(usage, 'candidates_token_count', 0))
                else:
                    prompt_len = len(prompt)
                    output_len = len(raw_text)
                    track_usage('extraction', max(1, prompt_len // 3), max(1, output_len // 4))
                parsed = parse_extraction_response(raw_text)
                if parsed is not None:
                    return parsed
            print(f'  [extract] Parse failed, attempt {attempt + 1}/{MAX_RETRIES}')
            if attempt < MAX_RETRIES - 1:
                sleep_with_backoff(attempt)
        except Exception as error:
            if is_retryable_error(str(error)):
                print(f'  [extract] Retryable: {str(error)[:80]}, attempt {attempt + 1}')
                if attempt < MAX_RETRIES - 1:
                    sleep_with_backoff(attempt)
            else:
                print(f'  [extract] Error: {str(error)[:200]}')
                if attempt < MAX_RETRIES - 1:
                    sleep_with_backoff(attempt)
    print(f'  [extract] Failed after {MAX_RETRIES} attempts')
    return empty_metadata()

def embed_text(client: genai.Client, text: str) -> List[float]:
    for attempt in range(MAX_RETRIES):
        try:
            response = client.models.embed_content(model=GEMINI_EMBEDDING_MODEL, contents=[text], config=genai_types.EmbedContentConfig(task_type='SEMANTIC_SIMILARITY', output_dimensionality=EMBED_DIM))
            embeddings = getattr(response, 'embeddings', None)
            if not embeddings or not isinstance(embeddings, list):
                raise RuntimeError('No embeddings returned')
            values = getattr(embeddings[0], 'values', None)
            if not values or not isinstance(values, list):
                raise RuntimeError('Missing values')
            usage = getattr(response, 'usage_metadata', None)
            if usage:
                track_usage('embedding', getattr(usage, 'prompt_token_count', 0))
            else:
                track_usage('embedding', max(1, len(text) // 3))
            return [float(v) for v in values]
        except Exception as error:
            if is_retryable_error(str(error)):
                print(f'  [embed] Retryable: {str(error)[:80]}, attempt {attempt + 1}')
                if attempt < MAX_RETRIES - 1:
                    sleep_with_backoff(attempt)
            else:
                print(f'  [embed] Error: {str(error)[:200]}')
                if attempt < MAX_RETRIES - 1:
                    sleep_with_backoff(attempt)
    print(f'  [embed] Failed after {MAX_RETRIES} attempts')
    return []

def ensure_temp_collection_indexes(database: Any, source_collection: Any, temp_collection_name: str) -> None:
    temp_collection = database[temp_collection_name]
    source_indexes = list(source_collection.list_indexes())
    for index in source_indexes:
        index_name = index.get('name', '')
        if index_name == '_id_':
            continue
        index_keys = list(index['key'].items())
        index_options = {}
        for option_key in ('unique', 'sparse', 'background', 'partialFilterExpression', 'expireAfterSeconds', 'hidden', 'storageEngine', 'weights', 'default_language', 'language_override', 'textIndexVersion', '2dsphereIndexVersion', 'bits', 'min', 'max', 'bucketSize'):
            if option_key in index:
                index_options[option_key] = index[option_key]
        try:
            temp_collection.create_index(index_keys, **index_options)
        except Exception as index_error:
            print(f'  [mongodb] Index {index_name} skipped: {index_error}')
    print(f'[mongodb] Indexes ready on {temp_collection_name}')

def insert_into_temp_collection(database: Any, temp_collection_name: str, records: List[Dict[str, Any]]) -> int:
    if not records:
        return 0
    temp_collection = database[temp_collection_name]
    documents = []
    operations = []
    for record in records:
        document = {'tags': record.get('tags', []) if record.get('_skipped') else _ensure_10_tags(record.get('tags') or []), 'brand_name': record.get('brand_name', []), 'associated_brand': record.get('associated_brand', []), 'associated_mention': record.get('associated_mention', []), 'hashtags': record.get('hashtags', []), 'is_sponsorship': record.get('is_sponsorship', 0), 'category': record.get('category', 'OTHER'), 'gemini_extraction_model': GEMINI_EXTRACTION_MODEL, 'gemini_processing_updated_at': time.time()}
        update_document: Dict[str, Any] = {'$set': document, '$unset': {'caption_is_specific': '', 'gemini_processing_status': '', 'gemini_processing_version': ''}}
        if not record.get('_skipped'):
            document['embedding'] = record.get('embedding', [])
            document['embedding_model'] = record.get('embedding_model', '')
            document['embedding_updated_at'] = record.get('embedding_updated_at', time.time())
        else:
            update_document['$unset'].update({'embedding': '', 'embedding_model': '', 'embedding_updated_at': ''})
        filter_document = {'_id': record['mongo_id']} if record.get('mongo_id') is not None else {'user_id': record.get('user_id', ''), 'post_id': record.get('post_id', '')}
        documents.append((filter_document, update_document, record))
        operations.append(UpdateOne(filter_document, update_document, upsert=False))
    if not operations:
        return 0
    try:
        result = temp_collection.bulk_write(operations, ordered=False)
        upserted = result.upserted_count
        modified = result.modified_count
        matched = result.matched_count
        print(f'[mongodb] {temp_collection_name}: {matched} matched, {modified} modified, {upserted} inserted')
        if matched != len(records):
            raise RuntimeError(f'MongoDB matched {matched}/{len(records)} records')
        return matched
    except Exception as error:
        print(f'[mongodb] bulkWrite error: {error}')
        count = 0
        for filter_document, update_document, record in documents:
            try:
                result = temp_collection.update_one(filter_document, update_document, upsert=False)
                count += int(result.matched_count)
            except Exception as single_error:
                print(f"[mongodb] Single upsert error for {record.get('user_id')}:{record.get('post_id')}: {single_error}")
        print(f'[mongodb] Upserted {count}/{len(documents)} individually')
        if count != len(documents):
            raise RuntimeError(f'MongoDB updated {count}/{len(documents)} records')
        return count

def save_local_backup(records: List[Dict[str, Any]], summary: Dict[str, Any]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(POSTS_BACKUP_FILE, 'w', encoding='utf-8') as f:
        json.dump({'metadata': summary, 'posts': records}, f, ensure_ascii=False, indent=2, default=str)
    with open(SUMMARY_OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f'[backup] Saved to {POSTS_BACKUP_FILE}')

def new_checkpoint_state(limit: int, location_id: int) -> Dict[str, Any]:
    return {'checkpoint_version': 2, 'database': MONGO_DATABASE, 'collection': MONGO_SOURCE_COLLECTION, 'processing_version': PROCESSING_VERSION, 'location_id': int(location_id), 'requested_limit': int(limit), 'total_processed': 0, 'total_enriched': 0, 'total_skipped': 0, 'total_errors': 0, 'total_sampled': 0, 'batch_number': 0, 'processing_time_seconds': 0.0, 'usage': get_usage_snapshot(), 'cost_estimate_usd': get_usage_summary(), 'updated_at': time.strftime('%Y-%m-%d %H:%M:%S')}

def load_checkpoint(limit: int, reset: bool, location_id: int) -> Dict[str, Any]:
    if reset and CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
    default_state = new_checkpoint_state(limit, location_id)
    if not CHECKPOINT_FILE.exists():
        return default_state
    try:
        with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
            state = json.load(f)
    except Exception:
        return default_state
    try:
        saved_location_id = int(state.get('location_id', -1))
    except (TypeError, ValueError):
        saved_location_id = -1
    if int(state.get('checkpoint_version', 0) or 0) != 2 or state.get('database') != MONGO_DATABASE or state.get('collection') != MONGO_SOURCE_COLLECTION or (state.get('processing_version') != PROCESSING_VERSION) or (saved_location_id != int(location_id)):
        return default_state
    state['requested_limit'] = int(limit)
    state['location_id'] = int(location_id)
    return state

def save_checkpoint(state: Dict[str, Any]) -> None:
    checkpoint_dir = CHECKPOINT_FILE.parent
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    state['usage'] = get_usage_snapshot()
    state['cost_estimate_usd'] = get_usage_summary()
    state['updated_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
    temp_file = CHECKPOINT_FILE.with_suffix(CHECKPOINT_FILE.suffix + '.tmp')
    with open(temp_file, 'w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=2, default=str)
    temp_file.replace(CHECKPOINT_FILE)

def process_posts(args: argparse.Namespace) -> None:
    source = args.source
    max_posts = max(0, int(args.limit))
    location_id = int(args.location_id)
    print('=' * 60)
    print('New External Helper Post Embedding Pipeline')
    print(f'  Source:        {source}')
    print(f'  Location ID:   {location_id}')
    print(f'  Post limit:    {max_posts}')
    print(f'  Batch size:    {args.batch_size}')
    print(f'  Workers:       {args.workers}')
    print(f'  Skip existing: {args.skip_existing}')
    print(f'  Extract model: {GEMINI_EXTRACTION_MODEL}')
    print(f'  Embed model:   {GEMINI_EMBEDDING_MODEL} (dim={EMBED_DIM})')
    print(f'  Source coll:   {MONGO_SOURCE_COLLECTION}')
    print(f'  Temp coll:     {MONGO_TEMP_COLLECTION}')
    print(f'  Database:      {MONGO_DATABASE}')
    print(f'  Checkpoint:    {CHECKPOINT_FILE}')
    print('=' * 60)
    if not GEMINI_API_KEY:
        print('FATAL: GEMINI_API_KEY not set in .env')
        sys.exit(1)
    gemini_client = genai.Client(api_key=GEMINI_API_KEY)
    checkpoint = load_checkpoint(max_posts, args.reset_checkpoint, location_id)
    restore_usage(checkpoint.get('usage', {}))
    mongo_client, mongo_database, mongo_source_collection = connect_mongodb()
    print(f'\n[setup] Ensuring indexes on {MONGO_TEMP_COLLECTION}...')
    ensure_temp_collection_indexes(mongo_database, mongo_source_collection, MONGO_TEMP_COLLECTION)
    existing_ids: set = set()
    if args.skip_existing:
        print(f'  [skip-existing] using extraction_model={GEMINI_EXTRACTION_MODEL} embedding_model={GEMINI_EMBEDDING_MODEL}')
    total_processed_global = int(checkpoint.get('total_processed', 0) or 0)
    total_enriched_global = int(checkpoint.get('total_enriched', 0) or 0)
    total_skipped_global = int(checkpoint.get('total_skipped', 0) or 0)
    total_errors_global = int(checkpoint.get('total_errors', 0) or 0)
    total_sampled_global = int(checkpoint.get('total_sampled', 0) or 0)
    batch_number = int(checkpoint.get('batch_number', 0) or 0)
    previous_elapsed = float(checkpoint.get('processing_time_seconds', 0.0) or 0.0)
    processed_records: List[Dict[str, Any]] = []
    overall_start = time.time()
    consecutive_failed_batches = 0
    print(f"[checkpoint] processed={total_processed_global}/{max_posts} enriched={total_enriched_global} skipped={total_skipped_global} errors={total_errors_global} cost=${get_usage_summary()['total_cost_usd']:.6f}")
    try:
        while total_processed_global < max_posts:
            batch_number += 1
            batch_remaining = max_posts - total_processed_global
            batch_take = min(max(1, int(args.batch_size)), batch_remaining)
            print(f"\n{'=' * 60}")
            print(f'Batch {batch_number} — fetching {batch_take} posts (total: {total_processed_global}/{max_posts})')
            print(f"{'=' * 60}")
            posts_to_process: List[Dict[str, Any]] = []
            if source == 'mongodb':
                mongo_records = fetch_mongodb_post_ids(mongo_source_collection, batch_take, location_id)
                if not mongo_records:
                    print('[batch] No more posts in MongoDB. Exiting batch loop.')
                    break
                for record in mongo_records:
                    posts_to_process.append(record)
            if existing_ids:
                before = len(posts_to_process)
                posts_to_process = [p for p in posts_to_process if (p['user_id'], p['post_id']) not in existing_ids]
                if before - len(posts_to_process):
                    print(f'  [skip-existing] skipped {before - len(posts_to_process)} already processed')
            if not posts_to_process:
                print('[batch] No new posts to process in this batch. Trying next batch.')
                consecutive_failed_batches += 1
                if consecutive_failed_batches >= 5:
                    break
                continue
            print(f'[batch] {len(posts_to_process)} posts to process')
            total_sampled_before_batch = total_sampled_global
            total_processed_before_batch = total_processed_global
            total_enriched_before_batch = total_enriched_global
            total_skipped_before_batch = total_skipped_global
            total_errors_before_batch = total_errors_global

            def checkpoint_callback(progress: Dict[str, int]) -> None:
                checkpoint['requested_limit'] = max_posts
                checkpoint['total_processed'] = total_processed_before_batch + int(progress.get('success_count', 0)) + int(progress.get('skip_count', 0))
                checkpoint['total_enriched'] = total_enriched_before_batch + int(progress.get('success_count', 0))
                checkpoint['total_skipped'] = total_skipped_before_batch + int(progress.get('skip_count', 0))
                checkpoint['total_errors'] = total_errors_before_batch + int(progress.get('error_count', 0))
                checkpoint['total_sampled'] = total_sampled_before_batch + int(progress.get('completed_count', 0))
                checkpoint['batch_number'] = batch_number
                checkpoint['processing_time_seconds'] = previous_elapsed + (time.time() - overall_start)
                save_checkpoint(checkpoint)
            batch_records = process_post_batch(gemini_client, mongo_database, MONGO_TEMP_COLLECTION, posts_to_process, args.workers, checkpoint_callback)
            processed_records.extend(batch_records['records'])
            total_enriched_global += batch_records['success_count']
            total_skipped_global += batch_records['skip_count']
            total_errors_global += batch_records['error_count']
            total_processed_global += batch_records['success_count'] + batch_records['skip_count']
            total_sampled_global += batch_records['completed_count']
            if batch_records['records']:
                for rec in batch_records['records']:
                    existing_ids.add((rec['user_id'], rec['post_id']))
            checkpoint['requested_limit'] = max_posts
            checkpoint['total_processed'] = total_processed_global
            checkpoint['total_enriched'] = total_enriched_global
            checkpoint['total_skipped'] = total_skipped_global
            checkpoint['total_errors'] = total_errors_global
            checkpoint['total_sampled'] = total_sampled_global
            checkpoint['batch_number'] = batch_number
            checkpoint['processing_time_seconds'] = previous_elapsed + (time.time() - overall_start)
            save_checkpoint(checkpoint)
            cost = get_usage_summary()
            print(f"[cost] extraction=${cost['extraction_cost_usd']:.6f} embedding=${cost['embedding_cost_usd']:.6f} total=${cost['total_cost_usd']:.6f}")
            if batch_records['success_count'] + batch_records['skip_count'] == 0:
                consecutive_failed_batches += 1
                if consecutive_failed_batches >= 5:
                    print('[batch] Five consecutive batches produced no completed posts. Exiting.')
                    break
            else:
                consecutive_failed_batches = 0
    finally:
        checkpoint['requested_limit'] = max_posts
        checkpoint['total_processed'] = max(int(checkpoint.get('total_processed', 0) or 0), total_processed_global)
        checkpoint['total_enriched'] = max(int(checkpoint.get('total_enriched', 0) or 0), total_enriched_global)
        checkpoint['total_skipped'] = max(int(checkpoint.get('total_skipped', 0) or 0), total_skipped_global)
        checkpoint['total_errors'] = max(int(checkpoint.get('total_errors', 0) or 0), total_errors_global)
        checkpoint['total_sampled'] = max(int(checkpoint.get('total_sampled', 0) or 0), total_sampled_global)
        checkpoint['batch_number'] = max(int(checkpoint.get('batch_number', 0) or 0), batch_number)
        checkpoint['processing_time_seconds'] = max(float(checkpoint.get('processing_time_seconds', 0.0) or 0.0), previous_elapsed + (time.time() - overall_start))
        save_checkpoint(checkpoint)
    overall_elapsed = previous_elapsed + (time.time() - overall_start)
    summary = {'total_posts': total_processed_global, 'total_enriched': total_enriched_global, 'total_skipped': total_skipped_global, 'total_errors': total_errors_global, 'total_sampled': total_sampled_global, 'session_records': len(processed_records), 'data_source': source, 'extraction_model': GEMINI_EXTRACTION_MODEL, 'embedding_model': GEMINI_EMBEDDING_MODEL, 'embedding_dimension': EMBED_DIM, 'processing_time_seconds': round(overall_elapsed, 2), 'mongodb_temp_collection': MONGO_TEMP_COLLECTION, 'mongodb_source_collection': MONGO_SOURCE_COLLECTION, 'mongodb_database': MONGO_DATABASE, 'checkpoint_file': str(CHECKPOINT_FILE), 'cost_estimate_usd': get_usage_summary()}
    save_local_backup(processed_records, summary)
    mongo_client.close()
    cost = get_usage_summary()
    print(f"\n{'=' * 60}")
    print('DONE')
    print(f'  Total batches:     {batch_number}')
    print(f'  Processed:         {total_processed_global}')
    print(f'  Enriched:          {total_enriched_global}')
    print(f'  Skipped:           {total_skipped_global}')
    print(f'  Errors:            {total_errors_global}')
    print(f'  Temp collection:   {MONGO_TEMP_COLLECTION}')
    print(f'  Checkpoint:        {CHECKPOINT_FILE}')
    print(f'  Time:              {overall_elapsed:.1f}s')
    print(f"  Cost (est.):       ${cost['total_cost_usd']:.4f}")
    print(f"    Extraction:      {cost['extraction_input_tokens']} in / {cost['extraction_output_tokens']} out tokens (${cost['extraction_cost_usd']:.4f})")
    print(f"    Embedding:       {cost['embedding_input_tokens']} tokens (${cost['embedding_cost_usd']:.4f})")
    print(f"{'=' * 60}")

def process_post_batch(gemini_client: genai.Client, mongo_database: Any, temp_collection_name: str, posts: List[Dict[str, Any]], workers: int, checkpoint_callback: Optional[Any]=None) -> Dict[str, Any]:
    if not posts:
        return {'records': [], 'success_count': 0, 'skip_count': 0, 'error_count': 0, 'completed_count': 0}
    total = len(posts)
    processed_records: List[Dict[str, Any]] = []
    counter_lock = threading.Lock()
    completed_count = 0
    success_count = 0
    skip_count = 0
    error_count = 0
    start_time = time.time()
    upsert_batch: List[Dict[str, Any]] = []
    upsert_lock = threading.Lock()

    def current_progress() -> Dict[str, int]:
        with counter_lock:
            return {'success_count': success_count, 'skip_count': skip_count, 'error_count': error_count, 'completed_count': completed_count}

    def flush_upsert_batch() -> None:
        with upsert_lock:
            if not upsert_batch:
                if checkpoint_callback is not None:
                    checkpoint_callback(current_progress())
                return
            batch = list(upsert_batch)
            upsert_batch.clear()
        insert_into_temp_collection(mongo_database, temp_collection_name, batch)
        if checkpoint_callback is not None:
            checkpoint_callback(current_progress())

    def process_one_post(post: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        caption = post['caption']
        if not is_meaningful_caption(caption):
            metadata = empty_metadata()
            return {'mongo_id': post.get('mongo_id'), 'user_id': post['user_id'], 'post_id': post['post_id'], 'locationId': post.get('locationId'), 'stats': post.get('stats'), 'caption': caption, 'tags': metadata['tags'], 'brand_name': metadata['brand_name'], 'associated_brand': metadata['associated_brand'], 'associated_mention': metadata['associated_mention'], 'hashtags': metadata['hashtags'], 'is_sponsorship': metadata['is_sponsorship'], 'category': metadata['category'], '_skipped': True}
        metadata = extract_metadata(gemini_client, caption)
        if not metadata.get('caption_is_specific', 0):
            return {'mongo_id': post.get('mongo_id'), 'user_id': post['user_id'], 'post_id': post['post_id'], 'locationId': post.get('locationId'), 'stats': post.get('stats'), 'caption': caption, 'tags': _ensure_10_tags(metadata.get('tags', [])), 'brand_name': [b for b in metadata.get('brand_name', []) if all((ord(c) < 128 for c in b))], 'associated_brand': metadata.get('associated_brand', []), 'associated_mention': metadata.get('associated_mention', []), 'hashtags': metadata.get('hashtags', []), 'is_sponsorship': metadata.get('is_sponsorship', 0), 'category': metadata.get('category', 'OTHER'), '_skipped': True}
        tags_text = ', '.join(metadata.get('tags', []))
        category = metadata.get('category', 'OTHER')
        _FILLER_TAGS = {'general topic', 'lifestyle', 'trending topic', 'pop culture', 'creator content', 'daily life', 'social media', 'video content', 'entertainment', 'travel update', 'personal thoughts', 'vacation', 'journey', 'travel experience', 'travel vlog', 'holiday', 'travel diary', 'travel lifestyle', 'travel planning', 'travel tips', 'daily update', 'life update', 'personal growth', 'life reflection', 'mindfulness', 'positive thinking', 'travel inspiration', 'travel memories', 'travel community', 'work life', 'family moment', 'food moment', 'music clip', 'tv show', 'shopping trip', 'city walk', 'pet moment', 'fitness update', 'road trip', 'travel moment', 'leisure time', 'travel photography', 'vacation mode', 'summer vacation', 'weekend getaway', 'daily routine', 'personal journey', 'life experience', 'travel lifestyle', 'travel destination'}
        raw_tags = metadata.get('tags', [])
        specific_tags = [t for t in raw_tags if t.lower() not in _FILLER_TAGS and all((ord(c) < 128 for c in t))]
        use_tags = len(specific_tags) >= 5
        if use_tags and tags_text:
            category_line = f'Category: {category}' if category and category != 'OTHER' else ''
            parts = [category_line, caption, f'Tags: {tags_text}'] if category_line else [caption, f'Tags: {tags_text}']
        else:
            parts = [caption]
        embedding_input = '\n\n'.join((p for p in parts if p))
        embedding_vector = embed_text(gemini_client, embedding_input)
        if not embedding_vector:
            return None
        return {'mongo_id': post.get('mongo_id'), 'user_id': post['user_id'], 'post_id': post['post_id'], 'locationId': post.get('locationId'), 'stats': post.get('stats'), 'caption': caption, 'tags': _ensure_10_tags(metadata['tags']), 'brand_name': [b for b in metadata.get('brand_name', []) if all((ord(c) < 128 for c in b))], 'associated_brand': metadata['associated_brand'], 'associated_mention': metadata['associated_mention'], 'hashtags': metadata['hashtags'], 'is_sponsorship': metadata['is_sponsorship'], 'category': metadata.get('category', 'OTHER'), 'embedding': embedding_vector, 'embedding_model': GEMINI_EMBEDDING_MODEL, 'embedding_updated_at': time.time()}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_post = {executor.submit(process_one_post, post): post for post in posts}
        for future in as_completed(future_to_post):
            post = future_to_post[future]
            post_label = f"{post['user_id']}:{post['post_id']}"
            try:
                record = future.result()
                with counter_lock:
                    completed_count += 1
                    if record is None:
                        error_count += 1
                    elif record.get('_skipped'):
                        processed_records.append(record)
                        skip_count += 1
                        with upsert_lock:
                            upsert_batch.append(record)
                    else:
                        processed_records.append(record)
                        success_count += 1
                        with upsert_lock:
                            upsert_batch.append(record)
                if completed_count > 0 and completed_count % 10 == 0:
                    flush_upsert_batch()
                if completed_count % 10 == 0 or completed_count == total:
                    elapsed = time.time() - start_time
                    rate = completed_count / elapsed if elapsed > 0 else 0
                    remaining = total - completed_count
                    eta = remaining / rate if rate > 0 else 0
                    cost = get_usage_summary()
                    print(f"[batch] {completed_count}/{total} | {rate:.2f} posts/s | ETA {eta:.0f}s | ok={success_count} skipped={skip_count} err={error_count} | cost=${cost['total_cost_usd']:.6f}")
            except Exception as e:
                with counter_lock:
                    completed_count += 1
                    error_count += 1
                print(f'  [error] {post_label}: {str(e)[:100]}')
    flush_upsert_batch()
    progress = current_progress()
    return {'records': processed_records, 'success_count': progress['success_count'], 'skip_count': progress['skip_count'], 'error_count': progress['error_count'], 'completed_count': progress['completed_count']}
def configure_country(location_id: int) -> None:
    # Processing functions import these values directly; refresh them together.
    global MONGO_SOURCE_COLLECTION, MONGO_TEMP_COLLECTION, CHECKPOINT_FILE
    global OUTPUT_DIR, POSTS_BACKUP_FILE, SUMMARY_OUTPUT_FILE
    settings.configure_location(location_id)
    MONGO_SOURCE_COLLECTION = settings.MONGO_SOURCE_COLLECTION
    MONGO_TEMP_COLLECTION = settings.MONGO_TEMP_COLLECTION
    CHECKPOINT_FILE = settings.CHECKPOINT_FILE
    OUTPUT_DIR = settings.OUTPUT_DIR
    POSTS_BACKUP_FILE = settings.POSTS_BACKUP_FILE
    SUMMARY_OUTPUT_FILE = settings.SUMMARY_OUTPUT_FILE
    restore_usage({})


def main() -> None:
    args = parse_arguments()
    locations = [3, 1, 4] if args.all_countries else [args.location_id]
    # Fail before API calls or writes if any requested country is unconfigured.
    missing = []
    for name, value in [('MONGO_URI_ATLAS_MYHKSG', MONGO_URI),
                        ('MONGO_DB_ATLAS_MYHKSG', MONGO_DATABASE),
                        ('GEMINI_API_KEY', GEMINI_API_KEY)]:
        if not value:
            missing.append(name)
    for location_id in locations:
        configure_country(location_id)
        if not MONGO_SOURCE_COLLECTION:
            missing.append(f'MONGO_COLL_NEW_INFLUENCER_{settings.COUNTRY_BY_LOCATION[location_id]}')
    if missing:
        raise SystemExit('Missing required environment variables: ' + ', '.join(missing))
    for location_id in locations:
        configure_country(location_id)
        country_args = argparse.Namespace(**vars(args))
        country_args.location_id = location_id
        print(f'[country] Starting {settings.COUNTRY_BY_LOCATION[location_id]}')
        process_posts(country_args)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\nInterrupted.')
        sys.exit(130)
    except Exception as e:
        print(f'\nFatal: {e}')
        sys.exit(1)
