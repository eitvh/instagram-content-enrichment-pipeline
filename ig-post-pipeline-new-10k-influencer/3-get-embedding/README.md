# New influencer metadata and embeddings ? step 3

Uses the original Gemini extraction prompt and embedding logic. Reads the posts imported by the new step 2 and updates those documents in place with metadata and embeddings.

Run all countries from the project root:

```bash
python ig-post-pipeline-new-10k-influencer/3-get-embedding/x-get-info.py --all-countries
```

Runs MY, HK, then SG sequentially. Country collections are selected from `MONGO_COLL_NEW_INFLUENCER_MY`, `MONGO_COLL_NEW_INFLUENCER_HK`, and `MONGO_COLL_NEW_INFLUENCER_SG`, using `MONGO_URI_ATLAS_MYHKSG` and `MONGO_DB_ATLAS_MYHKSG`. Each query also filters `locationId`. The date and External Helper selection come from step 2's imported dataset.

Single-country examples:

```bash
python ig-post-pipeline-new-10k-influencer/3-get-embedding/x-get-info.py --location-id 3
python ig-post-pipeline-new-10k-influencer/3-get-embedding/x-get-info.py --location-id 1
python ig-post-pipeline-new-10k-influencer/3-get-embedding/x-get-info.py --location-id 4
```

Without country flags, uses `DEFAULT_LOCATION_ID` from `.env` (fallback MY / 3). `--all-countries` and `--location-id` cannot be combined.

## Limits and resume

`--limit` applies **per country across checkpoint resumes**, like the original embedding step. The fallback is 500,000; `.env` `DEFAULT_LIMIT` can override it. For a ceiling of 1 million per country:

```bash
python ig-post-pipeline-new-10k-influencer/3-get-embedding/x-get-info.py --all-countries --limit 1000000
```

`--limit 0` processes no posts. `--batch-size` and `--workers` work as before. Already processed posts for the current models are filtered out. Short or nonspecific captions can be recorded as skipped without embeddings. API errors are retried by the existing processing logic.

Each country has separate local files under this step's `output/my`, `output/hk`, or `output/sg`: `gemini_checkpoint.json`, `posts.json`, and `processing_summary.json`. Backups contain the current session's records; checkpoint totals accumulate across resumes. `NEW_10K_GEMINI_OUTPUT_DIR` can change the output root. Original-flow output/checkpoint path variables are intentionally not reused.

`--reset-checkpoint` resets local counters for the selected countries; it does not clear model completion fields in MongoDB. Step 2's MongoDB checkpoint collections are not used by step 3. Fatal errors stop the country sequence; rerunning uses country-specific progress.

Requires `pymongo`, `google-genai`, and `python-dotenv`, with `GEMINI_API_KEY` and the existing Gemini model/settings variables in the root `.env`. Running this script sends captions to Gemini and uses the configured API account. The original pipeline files are unchanged.

## MongoDB fetch timeouts

Fetching lets MongoDB choose its index. Optional root `.env` settings:

```dotenv
MONGO_QUERY_TIMEOUT_MS=120000
MONGO_QUERY_ATTEMPTS=3
```

These defaults allow 120 seconds per query attempt and up to three attempts, with 2- and 4-second delays after timeouts. Failed cursors are closed and partial fetch results discarded before retrying. Other database errors still propagate. Exhausted retries stop the run using the existing checkpoint handling. Selection filters, processing rules, limits, and checkpoint format are unchanged. No database indexes are created by this change; query performance still depends on available indexes. An unhinted query may return eligible posts in a different order (no ordering was guaranteed before).
