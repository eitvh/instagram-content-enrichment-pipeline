# Instagram Caption Metadata and Embedding Pipeline

A Python pipeline that enriches existing Instagram post documents with structured metadata and vector embeddings using Google Gemini.

The pipeline reads captions from MongoDB Atlas, extracts searchable metadata, generates semantic embeddings, and writes the results back to a configured MongoDB collection. It also supports concurrent processing, resumable checkpoints, configurable cost estimation, caption-cleaning, and local JSON backups.

## Features

* Reads Instagram post documents from MongoDB Atlas.
* Randomly samples eligible posts in configurable batches.
* Filters posts by `locationId`.
* Extracts structured metadata from captions using Gemini.
* Generates vector embeddings for semantic search.
* Processes multiple posts concurrently.
* Tracks input and output token usage.
* Estimates extraction and embedding costs.
* Saves progress to a local checkpoint file.
* Resumes processing after interruption.
* Cleans whitespace and common text-encoding corruption.
* Saves processed records and a summary to local JSON files.
* Copies normal MongoDB indexes to a separate processing collection.
* Supports updating the source collection directly.

## Extracted Metadata

The Gemini extraction step produces:

* Exactly 10 English topic tags
* Brand names
* Associated brand Instagram usernames
* Associated influencer or creator usernames
* Original hashtags
* Sponsorship classification
* Caption-specificity classification
* One primary content category

Supported categories are:

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

The extraction prompt requires JSON-only output and requires all generated text to be in English.

## Requirements

* Python 3.10 or newer
* MongoDB Atlas or another compatible MongoDB deployment
* Google Gemini API access
* A MongoDB collection containing Instagram post documents
* MySQL access only when using the included MySQL helper functions

Python 3.10 or newer is required because the settings module uses union type syntax such as:

```python
int | None
```

## Installation

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

Install the dependencies:

```bash
pip install pymongo google-genai pymysql python-dotenv
```

Alternatively, create a `requirements.txt` file:

```text
pymongo
google-genai
PyMySQL
python-dotenv
```

Then run:

```bash
pip install -r requirements.txt
```

## Environment Configuration

Create a `.env` file in the project directory.

```dotenv
# MongoDB
MONGO_URI_ATLAS_SG=mongodb+srv://username:password@cluster.example.mongodb.net/
MONGO_DB_ATLAS_SG=ai-vector-search-transit
MONGO_COLL_ATLAS_SG=ig-post-sg
MONGO_TEMP_COLL_ATLAS_SG=ig-post-sg

# Gemini
GEMINI_API_KEY=your_gemini_api_key
GEMINI_EXTRACTION_MODEL=gemini-3.1-flash-lite
GEMINI_EMBEDDING_MODEL=gemini-embedding-2
EMBED_DIM=1536

# Processing
DEFAULT_LIMIT=500000
DEFAULT_BATCH_SIZE=1000
DEFAULT_LOCATION_ID=4
MAX_WORKERS=8
REQUEST_DELAY_SECONDS=0.25
MAX_RETRIES=5
MIN_CAPTION_LENGTH=20

# Cost estimation, USD per 1 million tokens
EXTRACTION_INPUT_PRICE=0.25
EXTRACTION_OUTPUT_PRICE=1.50
EMBEDDING_INPUT_PRICE=0.20

# Output
GEMINI_OUTPUT_DIR=./output
GEMINI_CHECKPOINT_FILE=./ig_post_sg_gemini_checkpoint.json

# Optional MySQL settings
CRAWL_DB_HOST=localhost
CRAWL_DB_PORT=3306
CRAWL_DB_USERNAME=my_database_user
CRAWL_DB_PASSWORD=my_database_password
CRAWL_DB_DATABASE=cloudbreakr_db2
```

Do not commit `.env` files or production credentials to source control.

## MongoDB Settings

### `MONGO_URI`

MongoDB connection URI.

Lookup order:

1. `MONGO_URI_ATLAS_SG`
2. `MONGO_URI_ATLAS`
3. Empty string

The application exits when no MongoDB URI is configured.

### `MONGO_DATABASE`

MongoDB database name.

Lookup order:

1. `MONGO_DB_ATLAS_SG`
2. `MONGO_DB_ATLAS`
3. `ai-vector-search-transit`

### `MONGO_SOURCE_COLLECTION`

Collection from which Instagram posts are sampled.

Lookup order:

1. `MONGO_COLL_ATLAS_SG`
2. `MONGO_COLL_ATLAS`
3. `ig-post-sg`

### `MONGO_TEMP_COLLECTION`

Collection that receives the metadata and embedding updates.

Lookup order:

1. `MONGO_TEMP_COLL_ATLAS_SG`
2. `MONGO_TEMP_COLLECTION`
3. The value of `MONGO_SOURCE_COLLECTION`

By default, the source and temporary collection are the same. This means the pipeline updates the source documents in place.

To write results to a separate collection, set:

```dotenv
MONGO_TEMP_COLL_ATLAS_SG=ig-post-sg-enriched
```

## Gemini Settings

| Variable                  |                 Default | Description                                              |
| ------------------------- | ----------------------: | -------------------------------------------------------- |
| `GEMINI_API_KEY`          |                    None | Gemini API key. Required.                                |
| `GEMINI_EXTRACTION_MODEL` | `gemini-3.1-flash-lite` | Model used to extract metadata.                          |
| `GEMINI_EMBEDDING_MODEL`  |    `gemini-embedding-2` | Model used to generate embeddings.                       |
| `EMBED_DIM`               |                  `1536` | Number of dimensions requested from the embedding model. |

The application exits before processing when `GEMINI_API_KEY` is missing.

The effective processing version is generated from:

```text
extraction model | embedding model | embedding dimension
```

For example:

```text
gemini-3.1-flash-lite|gemini-embedding-2|1536
```

Changing any of these values causes the existing local checkpoint to be treated as belonging to an older processing version.

## Processing Settings

| Variable                |  Default | Description                                                  |
| ----------------------- | -------: | ------------------------------------------------------------ |
| `DEFAULT_LIMIT`         | `500000` | Maximum number of completed posts across checkpoint resumes. |
| `DEFAULT_BATCH_SIZE`    |   `1000` | Documents sampled from MongoDB per batch.                    |
| `DEFAULT_LOCATION_ID`   |      `4` | Default `locationId` filter. Use `0` for all locations.      |
| `MAX_WORKERS`           |      `8` | Number of concurrent processing threads.                     |
| `REQUEST_DELAY_SECONDS` |   `0.25` | Configured request delay value.                              |
| `MAX_RETRIES`           |      `5` | Maximum Gemini extraction or embedding attempts.             |
| `MIN_CAPTION_LENGTH`    |     `20` | Minimum caption length required for meaningful processing.   |

Integer and floating-point settings are validated when `settings.py` is imported.

Invalid values produce errors such as:

```text
Invalid max workers value 'abc'. Use an integer.
```

Values below their configured minimum also produce an error.

## Running the Pipeline

Run the pipeline using its configured defaults:

```bash
python x-get-info.py
```

Process up to 100 posts:

```bash
python x-get-info.py --limit 100
```

Process 100 posts with five concurrent workers:

```bash
python x-get-info.py --limit 100 --workers 5
```

Use batches of 250 posts:

```bash
python x-get-info.py --limit 5000 --batch-size 250
```

Process only posts belonging to location `4`:

```bash
python x-get-info.py --location-id 4
```

Process posts from every location:

```bash
python x-get-info.py --location-id 0
```

Reset the local checkpoint and begin a new requested limit:

```bash
python x-get-info.py --reset-checkpoint --limit 2000
```

Run with the existing-post option:

```bash
python x-get-info.py --skip-existing
```

## Command-Line Options

| Option               | Description                                                                  |
| -------------------- | ---------------------------------------------------------------------------- |
| `--source mongodb`   | Selects the source of post documents. The current CLI supports only MongoDB. |
| `--limit N`          | Maximum total completed posts across checkpoint resumes.                     |
| `--batch-size N`     | Number of documents requested from MongoDB for each batch.                   |
| `--location-id N`    | Filters MongoDB documents by `locationId`. Use `0` for all locations.        |
| `--workers N`        | Number of concurrent processing threads.                                     |
| `--skip-existing`    | Enables the existing-post processing mode.                                   |
| `--reset-checkpoint` | Deletes the current checkpoint before processing.                            |

Display command help:

```bash
python x-get-info.py --help
```

## MongoDB Source Selection

The pipeline selects documents that:

* Have a non-empty string caption.
* Have not been processed with the configured extraction model.
* Do not have a Gemini processing timestamp.
* Have an embedding created by a different embedding model.
* Match the configured `locationId`, unless the location is `0`.

Eligible documents are randomly sampled using MongoDB's `$sample` stage.

The process uses two steps:

1. Randomly sample document `_id` values.
2. Retrieve the full documents using those `_id` values.

The records loaded for processing contain:

```text
mongo_id
user_id
post_id
locationId
stats
caption
```

## Caption Validation

A caption is considered meaningful only when:

* It is a string.
* It is not empty after trimming.
* Its length is at least `MIN_CAPTION_LENGTH`.
* It contains at least one alphabetic character.

Captions that fail these checks do not receive an embedding.

## Caption Cleaning

Before processing, captions are cleaned by:

* Removing null bytes.
* Converting Windows and old Mac line endings to `\n`.
* Reducing three or more consecutive newlines to two.
* Trimming whitespace from each line.
* Preserving paragraph separation.

The pipeline also attempts to repair mojibake caused by UTF-8 text being interpreted as Windows-1252 and encoded again.

Text that cannot be safely repaired is left unchanged.

## Metadata Extraction

For each meaningful caption, the extraction model is asked to return JSON containing:

```json
{
  "tags": [
    "Tag One",
    "Tag Two",
    "Tag Three",
    "Tag Four",
    "Tag Five",
    "Tag Six",
    "Tag Seven",
    "Tag Eight",
    "Tag Nine",
    "Tag Ten"
  ],
  "brand_name": [],
  "associated_brand": [],
  "associated_mention": [],
  "hashtags": [],
  "is_sponsorship": 0,
  "caption_is_specific": 1,
  "category": "OTHER"
}
```

The prompt requires:

* JSON only
* English-only text
* Exactly 10 tags
* Grounded tags supported by the caption
* No invented locations, people, brands, or events
* Brand and influencer handles in separate lists
* A binary sponsorship value
* A binary caption-specificity value
* Exactly one category

## Specific and Non-Specific Captions

After extraction, the pipeline checks `caption_is_specific`.

When it is `1`:

* Metadata is retained.
* An embedding is generated.
* The document is counted as enriched.

When it is `0`:

* Metadata is still written.
* The document is marked as skipped internally.
* No embedding is generated.
* Existing embedding fields are removed.

Captions that fail the initial meaningful-caption test are handled in the same skipped mode.

The `caption_is_specific` value itself is not stored permanently by the current update operation.

## Tag Handling

The pipeline filters generated tags to ASCII text.

It attempts to store exactly 10 tags:

* More than 10 valid tags are truncated.
* Fewer than 10 tags are padded.
* When no valid tags remain, the fallback value is repeated.

Possible fallback tags include:

```text
Content
Content 1
Content 2
```

Before adding tags to the embedding input, the pipeline removes generic filler tags. Tags are included in the embedding input only when at least five sufficiently specific English tags remain.

## Embedding Input

Depending on metadata quality, the embedding input is either:

```text
Caption text
```

or:

```text
Category: TRAVEL

Caption text

Tags: Okinawa Diving, Coral Reef, Scuba Diving, Japan Travel
```

The pipeline uses the Gemini embedding task type:

```text
SEMANTIC_SIMILARITY
```

The output dimensionality is controlled by `EMBED_DIM`.

## MongoDB Fields

Successfully enriched documents receive fields similar to:

```json
{
  "tags": [
    "Okinawa Diving",
    "Coral Reef",
    "Scuba Diving",
    "Japan Travel",
    "Ocean Wildlife",
    "Underwater Camera",
    "Marine Life",
    "Island Adventure",
    "Diving Experience",
    "Tropical Waters"
  ],
  "brand_name": [],
  "associated_brand": [],
  "associated_mention": [],
  "hashtags": [],
  "is_sponsorship": 0,
  "category": "TRAVEL",
  "gemini_extraction_model": "gemini-3.1-flash-lite",
  "gemini_processing_updated_at": 1786000000.0,
  "embedding": [
    0.0123,
    -0.0456,
    0.0789
  ],
  "embedding_model": "gemini-embedding-2",
  "embedding_updated_at": 1786000000.0
}
```

The example embedding is shortened. The actual number of values is controlled by `EMBED_DIM`.

The pipeline removes these older or unwanted fields:

```text
caption_is_specific
gemini_processing_status
gemini_processing_version
```

## Update Behavior

MongoDB updates match documents using:

```text
_id
```

When no MongoDB `_id` is available, the fallback match uses:

```text
user_id + post_id
```

The current implementation uses:

```python
upsert=False
```

Therefore:

* Existing matching documents are updated.
* Missing documents are not inserted.
* The update operation raises an error when the number of matched documents does not equal the number of processed records.

The code first attempts an unordered bulk write. If the bulk write fails, it retries each update individually.

## Collection Indexes

Before processing, the pipeline copies regular indexes from the source collection to the configured temporary collection.

The `_id_` index is skipped because MongoDB creates it automatically.

Standard index options such as the following are preserved where available:

```text
unique
sparse
partialFilterExpression
expireAfterSeconds
hidden
weights
default_language
2dsphereIndexVersion
```

MongoDB Atlas Search and vector-search indexes are not copied by this process. Create those separately through the MongoDB Atlas interface or API.

## Concurrent Processing

Posts are processed with `ThreadPoolExecutor`.

The number of workers is controlled by:

```dotenv
MAX_WORKERS=8
```

or:

```bash
python x-get-info.py --workers 8
```

Completed results are written to MongoDB in groups of 10 records.

Progress is also checkpointed after each write group.

## Retry Behavior

Gemini extraction and embedding operations retry up to `MAX_RETRIES` times.

Errors considered retryable include messages related to:

```text
429
quota
rate limit
timeout
deadline
unavailable
connection
temporarily unavailable
resource exhausted
```

The retry delay uses exponential backoff:

```text
1.5 seconds
3 seconds
6 seconds
12 seconds
24 seconds
48 seconds
60 seconds maximum
```

A metadata extraction that fails all attempts returns empty metadata.

An embedding operation that fails all attempts returns an empty vector and is counted as an error.

## Checkpointing

The default checkpoint file is:

```text
ig_post_sg_gemini_checkpoint.json
```

A custom path can be configured:

```dotenv
GEMINI_CHECKPOINT_FILE=/path/to/checkpoints/gemini.json
```

The checkpoint stores:

```text
database
collection
processing version
requested limit
total processed
total enriched
total skipped
total errors
total sampled
batch number
processing time
token usage
estimated cost
last update time
```

Checkpoint progress is written atomically:

1. Data is written to a temporary file.
2. The temporary file replaces the main checkpoint file.

A checkpoint is reset automatically when its:

* MongoDB database differs
* Source collection differs
* Processing version differs

Reset it manually with:

```bash
python x-get-info.py --reset-checkpoint
```

## Limit Behavior

The `--limit` value represents the total completed-post target across resumed runs.

For example:

```bash
python x-get-info.py --limit 5000
```

If a checkpoint already records 2,000 completed posts, the next run attempts to complete the remaining 3,000.

A completed post is one that is either:

* Successfully enriched
* Intentionally skipped because its caption is not suitable for embedding

Errors do not increase the completed-post total.

## Token and Cost Tracking

The pipeline tracks:

* Extraction input tokens
* Extraction output tokens
* Embedding input tokens

Estimated cost is calculated using:

```text
extraction input tokens / 1,000,000 × extraction input price
+ extraction output tokens / 1,000,000 × extraction output price
+ embedding input tokens / 1,000,000 × embedding input price
```

Default pricing configuration:

```dotenv
EXTRACTION_INPUT_PRICE=0.25
EXTRACTION_OUTPUT_PRICE=1.50
EMBEDDING_INPUT_PRICE=0.20
```

These are configurable estimates. They should be updated whenever the model provider's pricing changes.

When the Gemini response does not include usage metadata, token counts are estimated from text length.

## Output Files

The default output directory is:

```text
output/
```

Configure another directory with:

```dotenv
GEMINI_OUTPUT_DIR=/path/to/output
```

### `posts.json`

Contains metadata about the run and records processed during the current session:

```json
{
  "metadata": {
    "total_posts": 100,
    "total_enriched": 85,
    "total_skipped": 10,
    "total_errors": 5
  },
  "posts": []
}
```

### `processing_summary.json`

Contains the final processing summary without the full post list.

The summary includes:

```text
total posts
total enriched
total skipped
total errors
total sampled
session record count
data source
extraction model
embedding model
embedding dimension
processing time
MongoDB collections
checkpoint path
estimated cost
```

## Example Console Output

```text
============================================================
Embedding Test Pipeline
  Source:        mongodb
  Post limit:    100
  Batch size:    25
  Workers:       5
  Skip existing: False
  Extract model: gemini-3.1-flash-lite
  Embed model:   gemini-embedding-2 (dim=1536)
  Source coll:   ig-post-sg
  Temp coll:     ig-post-sg
  Database:      ai-vector-search-transit
============================================================

Batch 1 — fetching 25 posts (total: 0/100)
[batch] 10/25 | 1.20 posts/s | ETA 12s | ok=8 skipped=1 err=1 | cost=$0.001240
[mongodb] ig-post-sg: 10 matched, 9 modified, 0 inserted
[cost] extraction=$0.001100 embedding=$0.000140 total=$0.001240

============================================================
DONE
  Total batches:     4
  Processed:         100
  Enriched:          88
  Skipped:           9
  Errors:            3
  Temp collection:   ig-post-sg
  Time:              102.4s
  Cost (est.):       $0.0128
============================================================
```

## MySQL Settings

The settings module also defines a MySQL connection:

| Setting  | Lookup order or default                                                  |
| -------- | ------------------------------------------------------------------------ |
| Host     | `CRAWL_DB_HOST`, `CRAWL_DB_READ_HOST`, `crawl_db_hostname`, `localhost`  |
| Port     | `CRAWL_DB_PORT`, `db_port`, `3306`                                       |
| User     | `CRAWL_DB_USERNAME`, `CRAWL_DB_USER`, `crawl_db_username`                |
| Password | `CRAWL_DB_PASSWORD`, `crawl_db_password`                                 |
| Database | `CRAWL_DB_DATABASE`, `CRAWL_DB_NAME`, `crawl_db_name`, `cloudbreakr_db2` |

The main script contains MySQL caption-loading helper functions. However, the current command-line `--source` argument accepts only:

```text
mongodb
```

The MySQL helpers are therefore not selected through the current CLI without additional code changes.

## Important Implementation Notes

### Source and temporary collections

The temporary collection defaults to the source collection. Set `MONGO_TEMP_COLL_ATLAS_SG` when a separate destination is required.

### Existing-post option

The MongoDB source query already filters documents based on the configured extraction and embedding model fields.

In the current implementation, `--skip-existing` displays the selected models, while the in-memory existing-ID set is populated with records processed during the current run.

### Random selection

MongoDB uses `$sample`, so repeated runs may select different eligible posts.

### Vector-search index

Generating embeddings does not automatically create a MongoDB Atlas vector-search index. Configure the vector index separately using the same number of dimensions as `EMBED_DIM`.

### Model and dimension compatibility

Changing `EMBED_DIM` requires the Atlas vector-search index configuration to use the same vector dimension.

### Cost estimates

The cost output is an estimate based on configured rates and available or approximated token counts. It is not an official billing statement.

## Recommended `.gitignore`

```gitignore
.env
.venv/
venv/
__pycache__/
*.pyc
*.pyo
*.log

output/
ig_post_sg_gemini_checkpoint.json
ig_post_sg_gemini_checkpoint.json.tmp

.idea/
.vscode/
```

## Security Notes

* Never commit API keys or database credentials.
* Restrict MongoDB access using Atlas network rules.
* Use a MongoDB account with only the required permissions.
* URL-encode special characters in MongoDB usernames and passwords.
* Rotate credentials immediately when they are exposed.
* Avoid storing secrets in command history or application logs.
* Review the destination collection before running large jobs.

## Production Checklist

Before running a large processing job:

1. Test with a small limit:

   ```bash
   python x-get-info.py --limit 10 --batch-size 5 --workers 2
   ```

2. Inspect the updated MongoDB documents.

3. Confirm that embeddings contain `EMBED_DIM` values.

4. Confirm that metadata fields contain the expected English output.

5. Check the generated cost estimate.

6. Verify the checkpoint and output files.

7. Create or verify the MongoDB Atlas vector-search index.

8. Increase the limit, batch size, and worker count gradually.

## Troubleshooting

### Missing Gemini key

```text
FATAL: GEMINI_API_KEY not set in .env
```

Set:

```dotenv
GEMINI_API_KEY=your_api_key
```

### Missing MongoDB URI

```text
FATAL: MONGO_URI_ATLAS_SG is not set in .env
```

Set either:

```dotenv
MONGO_URI_ATLAS_SG=...
```

or:

```dotenv
MONGO_URI_ATLAS=...
```

### MongoDB connection timeout

Check:

* MongoDB URI
* Atlas IP access list
* Cluster status
* DNS connectivity
* Username and password

### Invalid numeric configuration

```text
Invalid embedding dimension value 'abc'. Use an integer.
```

Use a valid numeric value in `.env`.

### No posts selected

Possible causes include:

* No documents have non-empty captions.
* The location filter matches no documents.
* All documents already use the configured models.
* All eligible documents already have processing timestamps.
* The source collection name is incorrect.

### MongoDB matched fewer records than expected

The update operation requires every processed record to match an existing document.

Check:

* The source and temporary collections
* The document `_id`
* The `user_id` and `post_id` values
* Whether a separate temporary collection already contains corresponding documents

### Too many API errors

Reduce concurrency:

```bash
python x-get-info.py --workers 2
```

Also review API quota and retry settings.

## License

Add the appropriate license for your project.
