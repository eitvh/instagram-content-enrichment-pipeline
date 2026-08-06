# Instagram Post Data Pipeline

An end-to-end Python pipeline for collecting, migrating, enriching, and preparing Instagram post data for semantic search and influencer discovery.

The project:

1. Counts influencers and posts in MySQL.
2. Migrates Instagram posts from MySQL to MongoDB.
3. Extracts structured caption metadata using Gemini.
4. Generates vector embeddings for MongoDB Atlas Vector Search.

## Pipeline Overview

```text
MySQL
  │
  ├── Count influencers and posts
  │
  └── Sync Instagram posts
          │
          ▼
MongoDB Atlas
          │
          ├── Caption cleaning
          ├── Gemini metadata extraction
          ├── Vector embedding generation
          └── Semantic search-ready documents
```

## Main Features

* Filters influencers by location.
* Filters posts by date range.
* Limits posts per influencer.
* Migrates Instagram posts from MySQL to MongoDB.
* Repairs common caption-encoding problems.
* Uses MongoDB upserts to avoid duplicate documents.
* Saves checkpoints for resumable processing.
* Extracts tags, brands, mentions, hashtags, sponsorship status, and categories.
* Generates Gemini vector embeddings.
* Processes posts concurrently in batches.
* Tracks Gemini token usage and estimated cost.
* Saves local JSON backups and processing summaries.

## Project Structure

```text
project/
├── count_posts.py
├── sync_posts.py
├── x-get-info.py
├── settings.py
├── prompt.py
├── evaluation.py
├── requirements.txt
├── .env
├── .gitignore
├── output/
│   ├── posts.json
│   └── processing_summary.json
└── README.md
```

Rename the filenames in this README when the actual script names are different.

## Requirements

* Python 3.10 or newer
* MySQL
* MongoDB Atlas
* Google Gemini API key

Python 3.10 or newer is required because the settings module uses type syntax such as:

```python
int | None
```

## Installation

### 1. Open the project directory

```bash
cd path/to/project
```

### 2. Create a virtual environment

```bash
python -m venv .venv
```

### 3. Activate the virtual environment

On macOS or Linux:

```bash
source .venv/bin/activate
```

On Windows Command Prompt:

```cmd
.venv\Scripts\activate
```

On Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

After activation, the terminal should display `(.venv)` before the command prompt.

### 4. Upgrade pip

```bash
python -m pip install --upgrade pip
```

### 5. Install the required packages

```bash
python -m pip install python-dotenv pymongo pymysql google-genai
```

The packages are used for:

| Package         | Purpose                                             |
| --------------- | --------------------------------------------------- |
| `python-dotenv` | Loads environment variables from `.env`.            |
| `pymongo`       | Connects to and updates MongoDB.                    |
| `pymysql`       | Connects to MySQL.                                  |
| `google-genai`  | Uses Gemini for metadata extraction and embeddings. |

### 6. Create `requirements.txt`

```bash
python -m pip freeze > requirements.txt
```

Future installations can use:

```bash
python -m pip install -r requirements.txt
```

### 7. Deactivate the environment

```bash
deactivate
```

## Environment Configuration

Create a `.env` file in the project directory:

```dotenv
# MySQL
CRAWL_DB_HOST=localhost
CRAWL_DB_PORT=3306
CRAWL_DB_USERNAME=my_mysql_user
CRAWL_DB_PASSWORD=my_mysql_password
CRAWL_DB_DATABASE=my_mysql_database

# MongoDB
MONGO_URI_ATLAS_SG=mongodb+srv://username:password@cluster.mongodb.net/
MONGO_DB_ATLAS_SG=ai-vector-search-transit
MONGO_COLL_ATLAS_SG=ig-post-sg
MONGO_TEMP_COLL_ATLAS_SG=ig-post-sg
MONGO_CHECKPOINT_COLL=ig-post-sg-checkpoint

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

# Cost estimation in USD per 1 million tokens
EXTRACTION_INPUT_PRICE=0.25
EXTRACTION_OUTPUT_PRICE=1.50
EMBEDDING_INPUT_PRICE=0.20

# Output
GEMINI_OUTPUT_DIR=./output
GEMINI_CHECKPOINT_FILE=./ig_post_sg_gemini_checkpoint.json
```

Do not commit `.env` files or production credentials.

## Data Migration Settings

The MySQL-to-MongoDB migration uses settings similar to:

```python
LOCATION_ID = 4
POST_DATE_FROM = "2025-01-01"
POST_DATE_UNTIL = "2026-07-31"
LIMIT_PER_INFLUENCER = 10000

MAX_DOCS = 0
BATCH_SIZE = 500
```

| Setting                | Description                                                        |
| ---------------------- | ------------------------------------------------------------------ |
| `LOCATION_ID`          | Location used to select influencers.                               |
| `POST_DATE_FROM`       | First post date to include.                                        |
| `POST_DATE_UNTIL`      | Final post date to include.                                        |
| `LIMIT_PER_INFLUENCER` | Maximum posts loaded for each influencer.                          |
| `MAX_DOCS`             | Maximum documents processed during one run. Use `0` for unlimited. |
| `BATCH_SIZE`           | Number of MongoDB operations in each write batch.                  |

Dates should normally use:

```text
YYYY-MM-DD
```

## Usage

### 1. Count Influencers and Posts

```bash
python count_posts.py
```

The script prints:

```text
<number of influencers>
<number of posts>
```

Configuration and progress messages are written to standard error.

Example:

```text
[info] locationId=4 influencers(with ig_user)=125 from=2025-01-01 until=2026-07-31
[progress] 10/125 total_posts_so_far=512
125
6842
```

### 2. Sync MySQL Posts to MongoDB

```bash
python sync_posts.py
```

This stage:

* Reads influencers from MySQL.
* Reads Instagram posts for each influencer.
* Cleans caption encoding.
* Converts rows into MongoDB documents.
* Upserts documents using `user_id` and `post_id`.
* Saves completed-influencer checkpoints.
* Skips completed influencers on future runs.

Example MongoDB document:

```json
{
  "user_id": "12345",
  "post_id": "987654321",
  "caption": "Instagram caption",
  "locationId": 4,
  "stats": {
    "identityId": "456",
    "followerCount": 25000,
    "likeCount": 1200,
    "commentCount": 45,
    "postDate": "2026-07-20T14:30:00"
  }
}
```

### 3. Extract Metadata and Generate Embeddings

Run using the configured defaults:

```bash
python x-get-info.py
```

Process 100 posts:

```bash
python x-get-info.py --limit 100
```

Process 100 posts with five workers:

```bash
python x-get-info.py --limit 100 --workers 5
```

Use a custom batch size:

```bash
python x-get-info.py --limit 5000 --batch-size 250
```

Process location `4`:

```bash
python x-get-info.py --location-id 4
```

Process every location:

```bash
python x-get-info.py --location-id 0
```

Reset the local checkpoint:

```bash
python x-get-info.py --reset-checkpoint --limit 2000
```

Display command help:

```bash
python x-get-info.py --help
```

## Gemini Command-Line Options

| Option               | Description                                               |
| -------------------- | --------------------------------------------------------- |
| `--limit N`          | Maximum total completed posts across checkpoint resumes.  |
| `--batch-size N`     | Number of MongoDB documents loaded per batch.             |
| `--location-id N`    | Filters posts by `locationId`. Use `0` for all locations. |
| `--workers N`        | Number of concurrent processing workers.                  |
| `--skip-existing`    | Enables existing-post skipping behavior.                  |
| `--reset-checkpoint` | Deletes the current local checkpoint before processing.   |

## Extracted Metadata

Gemini adds fields such as:

```json
{
  "tags": [
    "Okinawa Diving",
    "Coral Reef",
    "Scuba Diving",
    "Japan Travel"
  ],
  "brand_name": [],
  "associated_brand": [],
  "associated_mention": [],
  "hashtags": [],
  "is_sponsorship": 0,
  "category": "TRAVEL",
  "embedding": [],
  "embedding_model": "gemini-embedding-2",
  "gemini_extraction_model": "gemini-3.1-flash-lite"
}
```

Supported categories include:

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

Gemini-generated text is expected to be in English.

## Caption Processing

Before extraction and embedding, captions are cleaned by:

* Removing null bytes.
* Normalizing line endings.
* Trimming whitespace.
* Preserving paragraph separation.
* Repairing common UTF-8 and Windows-1252 encoding corruption.

A caption is considered meaningful when:

* It is a non-empty string.
* Its length is at least `MIN_CAPTION_LENGTH`.
* It contains at least one alphabetic character.

Captions that are too short or non-specific are skipped for embedding.

## MongoDB Update Behavior

Migration documents are identified by:

```text
user_id + post_id
```

Gemini enrichment normally updates documents using MongoDB `_id`.

The pipeline adds or updates fields such as:

```text
tags
brand_name
associated_brand
associated_mention
hashtags
is_sponsorship
category
embedding
embedding_model
embedding_updated_at
gemini_extraction_model
gemini_processing_updated_at
```

The source and destination collections are the same by default:

```dotenv
MONGO_COLL_ATLAS_SG=ig-post-sg
MONGO_TEMP_COLL_ATLAS_SG=ig-post-sg
```

To write enrichment results to another collection:

```dotenv
MONGO_TEMP_COLL_ATLAS_SG=ig-post-sg-enriched
```

## Checkpoints and Resume Support

Both the migration and Gemini processing stages support checkpoints.

Checkpoints can store:

* Completed influencers
* Total processed posts
* Total enriched posts
* Skipped captions
* Errors
* Batch number
* Processing model version
* Token usage
* Estimated cost
* Processing time

The Gemini checkpoint defaults to:

```text
ig_post_sg_gemini_checkpoint.json
```

Reset it intentionally with:

```bash
python x-get-info.py --reset-checkpoint
```

Changing the extraction model, embedding model, or embedding dimension creates a different processing version.

## Batch and Worker Settings

Migration batch size:

```python
BATCH_SIZE = 500
```

Gemini processing batch size:

```dotenv
DEFAULT_BATCH_SIZE=1000
```

Gemini worker count:

```dotenv
MAX_WORKERS=8
```

Start with smaller values during testing:

```bash
python x-get-info.py --limit 10 --batch-size 5 --workers 2
```

Increase the values gradually after confirming that MongoDB updates and Gemini responses are correct.

## Retry Behavior

Database and Gemini operations use retry handling.

Gemini errors that may be retried include:

```text
429
quota
rate limit
timeout
deadline
unavailable
connection error
resource exhausted
```

The retry count is controlled by:

```dotenv
MAX_RETRIES=5
```

## Token and Cost Tracking

The Gemini stage tracks:

* Extraction input tokens
* Extraction output tokens
* Embedding input tokens

Estimated cost is calculated using the configured prices:

```dotenv
EXTRACTION_INPUT_PRICE=0.25
EXTRACTION_OUTPUT_PRICE=1.50
EMBEDDING_INPUT_PRICE=0.20
```

These values represent estimated USD cost per one million tokens.

Update them when the model provider's pricing changes.

The reported values are estimates and are not official billing totals.

## Output Files

Gemini processing creates:

```text
output/posts.json
output/processing_summary.json
ig_post_sg_gemini_checkpoint.json
```

### `output/posts.json`

Contains the records processed during the current session and run metadata.

### `output/processing_summary.json`

Contains:

* Processed count
* Enriched count
* Skipped count
* Error count
* Processing time
* Extraction model
* Embedding model
* Embedding dimension
* MongoDB collection names
* Estimated cost

## MongoDB Atlas Vector Search

Embeddings use the configured dimension:

```dotenv
EMBED_DIM=1536
```

Create a MongoDB Atlas Vector Search index using:

```text
Field: embedding
Dimensions: 1536
Similarity: cosine
```

The vector index dimension must match `EMBED_DIM`.

Generating embeddings does not automatically create the MongoDB Atlas Vector Search index.

## Performance Notes

Recommended MySQL indexes may include:

```sql
CREATE INDEX idx_influencer_location_user
ON influencer (locationId, deleted_at, ig_user_id);

CREATE INDEX idx_ig_post_user_date
ON ig_post (ig_user_id, postDate);

CREATE INDEX idx_ig_post_id
ON ig_post (ig_post_id);
```

Review existing indexes before creating new ones.

For large processing jobs:

1. Test with a small limit.
2. Inspect MongoDB documents.
3. Confirm the embedding dimensions.
4. Check the estimated cost.
5. Increase the limit and worker count gradually.

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

## Troubleshooting

### Missing Python module

```text
ModuleNotFoundError
```

Activate the virtual environment and reinstall the dependencies:

```bash
source .venv/bin/activate
python -m pip install python-dotenv pymongo pymysql google-genai
```

On Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
python -m pip install python-dotenv pymongo pymysql google-genai
```

### Missing Gemini API key

```text
FATAL: GEMINI_API_KEY not set in .env
```

Add:

```dotenv
GEMINI_API_KEY=your_gemini_api_key
```

### Missing MongoDB URI

```text
FATAL: MONGO_URI_ATLAS_SG is not set in .env
```

Add:

```dotenv
MONGO_URI_ATLAS_SG=mongodb+srv://...
```

### MySQL access denied

Check:

* MySQL username
* MySQL password
* Host and port
* Database permissions
* Allowed IP address

### MongoDB connection timeout

Check:

* MongoDB URI
* Atlas network access list
* Database username and password
* Cluster availability
* Internet and DNS connectivity

### No posts selected

Possible causes:

* The location ID matches no documents.
* Captions are empty.
* Captions are shorter than `MIN_CAPTION_LENGTH`.
* Posts have already been processed with the configured models.
* The MongoDB collection name is incorrect.

### Gemini rate-limit errors

Reduce the worker count:

```bash
python x-get-info.py --workers 2
```

Also review the Gemini API quota and retry settings.

## Security

* Never commit `.env`.
* Keep Gemini and database credentials private.
* Use restricted database accounts.
* Enable TLS for MongoDB connections.
* Restrict MongoDB Atlas network access.
* URL-encode special characters in MongoDB passwords.
* Test with small limits before running a full migration.
* Rotate credentials immediately when they are exposed.

## License

Add the appropriate project license here.
