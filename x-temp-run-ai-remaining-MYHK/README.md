# Instagram Content Enrichment and Embedding Pipeline

This script continues the Instagram content enrichment process from previous runs.

It reads Instagram post records, retrieves their captions, extracts searchable metadata using Gemini, generates vector embeddings, and stores the enriched documents in the temporary MongoDB collection:

```text
ig-post-embeddings-test
```

The temporary collection is used to test semantic search and MongoDB Atlas Vector Search without modifying the production `ig-post` collection.

---

## Current Progress

| Location | `location_id` | Generated documents |
|---|---:|---:|
| Malaysia (MY) | 3 | 319,527 |
| Hong Kong (HK) | 1 | 184,778 |
| **Total generated** | — | **504,305** |

Total records currently available in `ig-post`:

```text
3,462,114
```

Estimated raw records not yet generated:

```text
3,462,114 - 504,305 = 2,957,809
```

Current raw completion percentage:

```text
504,305 / 3,462,114 ≈ 14.57%
```

> The remaining value is only a raw difference. Some posts may be skipped because they have missing, short, meaningless, or non-specific captions.

---

## What the Script Does

For every selected Instagram post, the pipeline:

1. Reads the post ID and basic information from MongoDB Atlas.
2. Retrieves the post caption from the MySQL `ig_post` table.
3. Cleans the caption and attempts to repair encoding issues.
4. Checks whether the caption contains meaningful content.
5. Sends the caption to Gemini for metadata extraction.
6. Generates topic tags, brand information, hashtags, sponsorship status, and category.
7. Skips captions that are not specific enough for semantic search.
8. Generates a Gemini vector embedding.
9. Stores the generated record locally before database insertion.
10. Upserts the record into `ig-post-embeddings-test`.
11. Saves checkpoints, progress, token usage, estimated cost, and processing time.

---

## Pipeline Flow

```text
MongoDB Atlas
ig-post
    │
    │ user_id, post_id, locationId, stats
    ▼
MySQL
ig_post
    │
    │ caption content
    ▼
Caption cleaning
    │
    ▼
Gemini metadata extraction
    │
    ├── Topic tags
    ├── Brand names
    ├── Brand accounts
    ├── Creator mentions
    ├── Hashtags
    ├── Sponsorship detection
    ├── Content specificity
    └── Content category
    │
    ▼
Gemini embedding generation
    │
    ▼
Local staging and checkpoints
    │
    ▼
MongoDB Atlas
ig-post-embeddings-test
```

---

## Generated Metadata

The Gemini extraction process generates:

| Field | Description |
|---|---|
| `caption` | Cleaned Instagram caption |
| `tags` | Exactly 10 English topic tags |
| `brand_name` | Brand names detected in the caption |
| `associated_brand` | Instagram usernames classified as brands |
| `associated_mention` | Instagram usernames classified as people or creators |
| `hashtags` | Original hashtags without the `#` symbol |
| `is_sponsorship` | `1` when promotional or sponsored, otherwise `0` |
| `caption_is_specific` | Whether the caption contains searchable information |
| `category` | Primary content category |
| `embedding` | Gemini vector embedding |
| `embedding_model` | Model used to generate the embedding |
| `embedding_updated_at` | Embedding generation timestamp |
| `gemini_extraction_model` | Model used for metadata extraction |
| `gemini_processing_updated_at` | Metadata processing timestamp |

---

## Supported Categories

Each processed caption is assigned exactly one category:

```text
BEAUTY_MAKEUP
TRAVEL
FOOD
FASHION
MUSIC
PERSONAL
SHOPPING
EDUCATION
NEWS
SPORTS
PET
OTHER
```

---

## Caption Processing Rules

A caption is considered meaningful when:

- It exists and is a string.
- It contains at least 20 characters.
- It contains alphabetic content.

A post is skipped when:

- Its caption is missing.
- Its caption is too short.
- Its caption does not contain meaningful text.
- Gemini classifies the caption as non-specific.
- The post already exists in `ig-post-embeddings-test`.
- The post was already handled in the local processed-state file.

---

## Embedding Input

When Gemini produces at least five useful, non-generic tags, the embedding input includes:

```text
Category: <category>

<caption>

Tags: <tag 1>, <tag 2>, ...
```

Otherwise, only the original cleaned caption is embedded.

The default embedding configuration is:

```text
Model: gemini-embedding-2
Dimensions: 1536
Task type: SEMANTIC_SIMILARITY
```

---

## Example MongoDB Document

```json
{
  "user_id": "123456",
  "post_id": "987654321",
  "locationId": 1,
  "stats": {},
  "caption": "Example Instagram caption",
  "tags": [
    "Hong Kong Food",
    "Restaurant Review",
    "Local Cuisine",
    "Dining Experience",
    "Food Recommendation",
    "Asian Cuisine",
    "City Dining",
    "Restaurant Visit",
    "Food Photography",
    "Hong Kong Restaurant"
  ],
  "brand_name": [],
  "associated_brand": [],
  "associated_mention": [],
  "hashtags": [],
  "is_sponsorship": 0,
  "category": "FOOD",
  "embedding": [
    0.0123,
    -0.0456
  ],
  "embedding_model": "gemini-embedding-2",
  "embedding_updated_at": 1786000000.0,
  "gemini_extraction_model": "gemini-3.1-flash-lite",
  "gemini_processing_updated_at": 1786000000.0
}
```

---

## Requirements

- Python 3.10 or newer
- MongoDB Atlas connection
- MySQL database connection
- Gemini API key

---

## Installation

Clone the repository:

```bash
git clone <repository-url>
cd instagram-content-enrichment-pipeline
```

Create a virtual environment:

```bash
python -m venv .venv
```

Activate it on macOS or Linux:

```bash
source .venv/bin/activate
```

Activate it on Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

Install the required packages:

```bash
pip install python-dotenv pymongo pymysql google-genai
```

---

## Environment Variables

Create a `.env` file in the same directory as `run-temp.py`.

```dotenv
# MongoDB Atlas
MONGO_URI_ATLAS=mongodb+srv://<username>:<password>@<cluster>/
MONGO_DB_NAME=ai-vector-search

# MySQL
CRAWL_DB_HOST=<mysql-host>
CRAWL_DB_PORT=3306
CRAWL_DB_USERNAME=<mysql-username>
CRAWL_DB_PASSWORD=<mysql-password>
CRAWL_DB_DATABASE=cloudbreakr_db2

# Gemini
GEMINI_API_KEY=<gemini-api-key>
GEMINI_EXTRACTION_MODEL=gemini-3.1-flash-lite
GEMINI_EMBEDDING_MODEL=gemini-embedding-2
EMBED_DIM=1536

# Processing
MAX_WORKERS=8
REQUEST_DELAY_SECONDS=0.25
MAX_RETRIES=5
MONGO_FLUSH_SIZE=20

# Cost estimation in USD per 1 million tokens
EXTRACTION_INPUT_PRICE=0.25
EXTRACTION_OUTPUT_PRICE=1.50
EMBEDDING_INPUT_PRICE=0.20
```

Do not commit credentials or the `.env` file.

Recommended `.gitignore`:

```gitignore
.env
.venv/
__pycache__/
output/
*.pyc
```

---

## Command-Line Arguments

| Argument | Description | Default |
|---|---|---|
| `--source` | Candidate source: `mongodb` or `mysql` | `mongodb` |
| `--limit` | Cumulative number of records to handle | `5` |
| `--batch-size` | Target records per batch | `1000` |
| `--location-id` | Location ID argument | `1` |
| `--workers` | Concurrent Gemini workers | `8` |
| `--skip-existing` | Skip existing destination records | Disabled |
| `--reset-checkpoint` | Reset selected local checkpoint files | Disabled |

---

## Usage

### Run a Small Test

Test the connections and generated output before starting a large run:

```bash
python run-temp.py \
  --source mongodb \
  --limit 100 \
  --batch-size 100 \
  --workers 4
```

### Continue the Previous Process

The script automatically loads existing checkpoint and processed-state files from the `output` directory.

For example, continue processing until the local handled total reaches 600,000:

```bash
python run-temp.py \
  --source mongodb \
  --limit 600000 \
  --batch-size 1000 \
  --workers 8
```

After reaching 600,000, continue to another milestone:

```bash
python run-temp.py \
  --source mongodb \
  --limit 700000 \
  --batch-size 1000 \
  --workers 8
```

### Run with MySQL as the Direct Source

```bash
python run-temp.py \
  --source mysql \
  --location-id 1 \
  --limit 2000 \
  --batch-size 1000 \
  --workers 8
```

### Reset Local Progress

```bash
python run-temp.py \
  --reset-checkpoint \
  --source mongodb \
  --limit 100
```

> Resetting the checkpoint removes selected progress files but does not remove the staged-record file.

---

## Understanding `--limit`

The `--limit` value represents the cumulative number of terminal local statuses:

```text
success + failed + skipped
```

It does not necessarily represent newly generated embeddings.

For example, the following records may count toward the limit:

- Successfully inserted records
- Existing records that were skipped
- Non-meaningful captions
- Non-specific captions
- Failed Gemini processing attempts

Use the MongoDB destination collection count to determine the actual number of generated records.

---

## Local Output Files

Runtime files are stored in:

```text
output/
```

| File | Purpose |
|---|---|
| `processed_postid.json` | Tracks successful, failed, and skipped post IDs |
| `temp_generated_records.json` | Stores generated records waiting for MongoDB insertion |
| `checkpoint.json` | Stores batch, offset, elapsed time, usage, cost, and stop information |
| `realtime_summary.json` | Stores live progress, ETA, tokens, and estimated cost |
| `posts.json` | Session backup containing generated records and final summary |

Example directory:

```text
output/
├── checkpoint.json
├── posts.json
├── processed_postid.json
├── realtime_summary.json
└── temp_generated_records.json
```

---

## Resume and Recovery

The script is designed to preserve progress during interruptions.

### Keyboard Interruption

Press:

```text
Ctrl+C
```

The script will:

- Record the interruption reason.
- Preserve staged records.
- Save the current checkpoint.
- Save the processed-post state.
- Write a final summary.
- Close the MongoDB connection.

Run the same command again to continue.

### Gemini API Limit

The pipeline stops automatically when it detects errors such as:

```text
429
Resource exhausted
Quota exceeded
Insufficient credits
Billing disabled
Payment required
```

Checkpoint and staged data are preserved.

### Temporary API Errors

Temporary errors are retried using exponential backoff, including:

```text
Timeout
Connection error
500
502
503
504
Service unavailable
```

---

## MongoDB Write Behaviour

Generated records are staged locally before being written to MongoDB.

The pipeline:

1. Stages each generated document locally.
2. Flushes records when the staging size reaches `MONGO_FLUSH_SIZE`.
3. Uses unordered MongoDB bulk upserts.
4. Uses `post_id` as the upsert lookup field.
5. Checks whether each document exists after the bulk write.
6. Retries unsuccessful documents individually.
7. Stops and preserves staged data when MongoDB insertion fails.

This prevents a generated embedding from being lost when a database operation fails.

---

## Monitoring Progress

MongoDB shell examples:

```javascript
// Total generated documents
db.getCollection("ig-post-embeddings-test").countDocuments({})
```

```javascript
// Malaysia documents
db.getCollection("ig-post-embeddings-test").countDocuments({
  locationId: 3
})
```

```javascript
// Hong Kong documents
db.getCollection("ig-post-embeddings-test").countDocuments({
  locationId: 1
})
```

```javascript
// Total source documents
db.getCollection("ig-post").countDocuments({})
```

The terminal and `output/realtime_summary.json` show:

- Total processed
- Successful records
- Failed records
- Skipped records
- Pending MongoDB records
- Processing rate
- Estimated completion time
- Gemini token usage
- Estimated extraction cost
- Estimated embedding cost
- Combined estimated cost
- Last stop reason

---

## MongoDB Indexes

The script copies regular indexes from `ig-post` to:

```text
ig-post-embeddings-test
```

It also attempts to create:

```text
post_id_lookup
```

The script does not create the MongoDB Atlas Vector Search index. That index must be created separately.

Example vector field configuration:

```json
{
  "type": "vector",
  "path": "embedding",
  "numDimensions": 1536,
  "similarity": "cosine"
}
```

The index dimension must match:

```dotenv
EMBED_DIM=1536
```

---

## Important Implementation Notes

### `--location-id` Does Not Currently Filter the MySQL Query

The script accepts:

```text
--location-id
```

However, the current direct MySQL query does not contain a location condition.

Therefore, this command does not guarantee that only Hong Kong posts are processed:

```bash
python run-temp.py \
  --source mysql \
  --location-id 1 \
  --limit 2000
```

The MySQL query should be updated with the correct location column before running separate MY and HK processes.

### MongoDB Mode Uses Random Sampling

MongoDB source mode uses `$sample` to select candidate posts.

This works well during testing, but when most records have already been processed, random sampling may repeatedly return:

- Existing destination records
- Previously handled posts
- Posts without usable captions

The pipeline stops after ten consecutive batches without new processable records.

A deterministic query for missing records would be more reliable for a complete backfill.

### Direct MySQL Mode Requires Latin Characters

The current MySQL query contains:

```sql
content REGEXP '[a-zA-Z]'
```

Captions containing only Chinese or other non-Latin characters will not be selected in direct MySQL mode.

### Existing Records Are Always Checked

Before sending a post to Gemini, the pipeline checks whether its `post_id` already exists in `ig-post-embeddings-test`.

Existing posts are skipped to avoid:

- Duplicate Gemini requests
- Duplicate embeddings
- Unnecessary API cost
- Duplicate destination documents

### Checkpoint Reset Does Not Remove Staged Records

`--reset-checkpoint` removes:

```text
processed_postid.json
checkpoint.json
realtime_summary.json
```

It does not remove:

```text
temp_generated_records.json
```

This protects generated records that have not yet been inserted into MongoDB.

---

## Recommended Run Procedure

Before starting a long process:

1. Confirm the destination is `ig-post-embeddings-test`.
2. Back up the existing `output` directory.
3. Check current MY and HK destination counts.
4. Run a test with 50–100 records.
5. Inspect generated tags and categories.
6. Confirm that embeddings contain 1536 values.
7. Check the Gemini quota and billing configuration.
8. Start with four workers.
9. Increase to eight workers only when API limits are stable.
10. Monitor `realtime_summary.json` and MongoDB counts.

---

## Current Processing Targets

Current generated total:

```text
504,305
```

Suggested milestone runs:

```text
600,000
700,000
800,000
1,000,000
```

Example next run:

```bash
python run-temp.py \
  --source mongodb \
  --limit 600000 \
  --batch-size 1000 \
  --workers 8
```

After completion, verify the real destination count:

```javascript
db.getCollection("ig-post-embeddings-test").countDocuments({})
```

---

## Safety Notes

- Always run against `ig-post-embeddings-test`.
- Do not change the destination to production without validation.
- Never commit database credentials.
- Back up checkpoint files before changing processing logic.
- Test prompt changes on a small sample.
- Verify embedding dimensions before creating a vector index.
- Monitor Gemini usage to avoid unexpected API costs.

---

## License

Internal project use.