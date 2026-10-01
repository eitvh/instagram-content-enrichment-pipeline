# Instagram Post MySQL-to-MongoDB Sync

A Python command-line utility that retrieves Instagram posts from a MySQL database and upserts them into a MongoDB collection.

The script processes influencers belonging to a configured location, filters their posts by date, repairs common text-encoding problems, writes documents in batches, and records checkpoints so completed influencers can be skipped on future runs.

## Features

* Retrieves active influencers from MySQL by location.
* Fetches posts within a configurable date range.
* Limits the number of posts retrieved per influencer.
* Upserts posts into MongoDB instead of creating duplicates.
* Uses a unique key based on `user_id` and `post_id`.
* Repairs common mojibake and character-encoding problems in captions.
* Writes MongoDB documents in configurable batches.
* Tracks completed influencers in a checkpoint collection.
* Resumes interrupted jobs using saved checkpoints.
* Supports a global document limit for testing or partial runs.
* Retries failed MySQL operations using exponential backoff.
* Displays live progress and completion statistics.
* Loads credentials from environment variables or a `.env` file.


Replace `sync_instagram_posts.py` with the actual filename containing the `main()` function.

## Requirements

* Python 3.8 or newer
* MySQL or a MySQL-compatible database
* MongoDB or MongoDB Atlas
* Read access to the required MySQL tables
* Read and write access to the configured MongoDB database

The script uses these MySQL tables:

* `influencer`
* `ig_user`
* `ig_post`

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

Install the required dependencies:

```bash
pip install pymysql pymongo python-dotenv
```

Alternatively, create a `requirements.txt` file:

```text
PyMySQL
pymongo
python-dotenv
```

Install the dependencies from the file:

```bash
pip install -r requirements.txt
```

## Configuration

The application settings are defined in `settings.py`.

```python
# Post settings
LOCATION_ID = 4
POST_DATE_FROM = "2025-01-01"
POST_DATE_UNTIL = "2026-07-31"
LIMIT_PER_INFLUENCER = 10000
MAX_DOCS = 0
BATCH_SIZE = 500
```

### Post settings

| Setting                | Description                                                              |
| ---------------------- | ------------------------------------------------------------------------ |
| `LOCATION_ID`          | Location ID used to select influencers.                                  |
| `POST_DATE_FROM`       | First post date to include.                                              |
| `POST_DATE_UNTIL`      | Last post date to include.                                               |
| `LIMIT_PER_INFLUENCER` | Maximum posts retrieved for each influencer.                             |
| `MAX_DOCS`             | Maximum documents processed during one run. Use `0` for no global limit. |
| `BATCH_SIZE`           | Number of MongoDB upsert operations sent in each batch.                  |

### Date formats

The recommended date format is:

```text
YYYY-MM-DD
```

Example:

```python
POST_DATE_FROM = "2025-01-01"
POST_DATE_UNTIL = "2026-07-31"
```

The script also accepts `DD/MM/YYYY` values:

```python
POST_DATE_FROM = "01/01/2025"
POST_DATE_UNTIL = "31/07/2026"
```

These values are normalized internally to:

```text
2025-01-01
2026-07-31
```

Both `POST_DATE_FROM` and `POST_DATE_UNTIL` must be set. The program exits when either value is missing or empty.

### End-date behavior

The end date includes the entire configured day.

For example:

```python
POST_DATE_UNTIL = "2026-07-31"
```

The MySQL query uses a condition equivalent to:

```sql
ig_post.postDate < UNIX_TIMESTAMP(
    DATE_ADD('2026-07-31', INTERVAL 1 DAY)
)
```

This includes posts published throughout July 31 and excludes posts beginning on August 1.

## Environment Variables

Create a `.env` file in the project directory:

```dotenv
# MySQL
CRAWL_DB_HOST=localhost
CRAWL_DB_PORT=3306
CRAWL_DB_USERNAME=my_mysql_user
CRAWL_DB_PASSWORD=my_mysql_password
CRAWL_DB_DATABASE=my_mysql_database

# MongoDB
MONGO_URI_ATLAS_MYHKSG=mongodb+srv://username:password@cluster.example.mongodb.net/
MONGO_DB_ATLAS_MYHKSG=my_mongo_database
MONGO_COLL_ATLAS_MY=instagram_posts
MONGO_CHECKPOINT_COLL_MY=ig-post-my-checkpoint
```

Do not surround values with quotes unless those quotes are part of the actual value.

## MySQL Environment Variables

The following MySQL environment variable names are supported.

| Setting  | Lookup order                                                                 |
| -------- | ---------------------------------------------------------------------------- |
| Host     | `CRAWL_DB_HOST`, `CRAWL_DB_READ_HOST`, `crawl_db_hostname`, then `localhost` |
| Port     | `CRAWL_DB_PORT`, `db_port`, then `3306`                                      |
| Username | `CRAWL_DB_USERNAME`, then `crawl_db_username`                                |
| Password | `CRAWL_DB_PASSWORD`, then `crawl_db_password`                                |
| Database | `CRAWL_DB_DATABASE`, then `crawl_db_name`                                    |

The `CRAWL_DB_READ_HOST` variable allows the script to use a read replica when `CRAWL_DB_HOST` is not provided.

The port must be a valid integer:

```dotenv
CRAWL_DB_PORT=3306
```

An invalid value causes an error similar to:

```text
ValueError: Invalid database port value 'invalid'. Use an integer like '3306'.
```

## MongoDB Environment Variables

| Variable                | Description                                          | Required |
| ----------------------- | ---------------------------------------------------- | -------- |
| `MONGO_URI_ATLAS_MYHKSG`    | MongoDB or MongoDB Atlas connection string.          | Yes      |
| `MONGO_DB_ATLAS_MYHKSG`     | Target MongoDB database.                             | Yes      |
| `MONGO_COLL_ATLAS_MY`   | Collection receiving Instagram post documents.       | Yes      |
| `MONGO_CHECKPOINT_COLL_MY` | Collection storing completed-influencer checkpoints. | No       |

When `MONGO_CHECKPOINT_COLL_MY` is not set, the default collection name is:

```text
ig-post-my-checkpoint
```

The script validates these required settings before connecting:

* `CRAWL_DB_USERNAME`
* `CRAWL_DB_DATABASE`
* `MONGO_URI_ATLAS_MYHKSG`
* `MONGO_DB_ATLAS_MYHKSG`
* `MONGO_COLL_ATLAS_MY`

## Running the Script

Run the synchronization utility:

```bash
python sync_instagram_posts.py
```

The script writes progress information to standard error and prints the final prepared-document count to standard output.

## Example Output

```text
[info] locationId=4 influencers=125 completed=20 from=2025-01-01 until=2026-07-31 limit_per_influencer=10000 max_docs=0 batch_size=500
[progress] idx=45/125 processed=25 skipped=20 prepared=8412 upserted=5200 modified=2700 matched=3212 elapsed=1m42s
[done] influencers_total=125 processed=105 skipped=20 posts_prepared=32140 upserted=20400 modified=8300 matched=11740 elapsed=7m15s
32140
```

### Output fields

| Field         | Meaning                                                            |
| ------------- | ------------------------------------------------------------------ |
| `influencers` | Total influencers returned by MySQL.                               |
| `completed`   | Influencers already marked complete in the checkpoint collection.  |
| `processed`   | Influencers processed during the current run.                      |
| `skipped`     | Influencers skipped because a completed checkpoint already exists. |
| `prepared`    | Post documents prepared and sent to MongoDB.                       |
| `upserted`    | New MongoDB documents created.                                     |
| `modified`    | Existing MongoDB documents whose contents changed.                 |
| `matched`     | Existing MongoDB documents matched by an upsert operation.         |
| `elapsed`     | Total running time.                                                |

The final line contains only the number of prepared documents:

```text
32140
```

This makes the value easy to capture from another program.

## Redirecting Output

Save the final document count:

```bash
python sync_instagram_posts.py > result.txt
```

Save progress logs separately:

```bash
python sync_instagram_posts.py > result.txt 2> sync.log
```

Display and save progress simultaneously on macOS or Linux:

```bash
python sync_instagram_posts.py 2> >(tee sync.log >&2)
```

## Processing Workflow

The script performs the following steps:

1. Validates the required configuration.
2. Connects to MongoDB.
3. Creates the required MongoDB indexes.
4. Retrieves active influencers from MySQL.
5. Loads completed influencer IDs from the checkpoint collection.
6. Skips influencers that have already been completed.
7. Retrieves posts for each remaining influencer.
8. Converts MySQL rows into MongoDB documents.
9. Repairs common caption-encoding problems.
10. Upserts the documents into MongoDB in batches.
11. Records completed influencers in the checkpoint collection.
12. Prints progress and final statistics.
13. Closes the MongoDB connection.

## Influencer Selection

An influencer is selected when:

* `influencer.deleted_at` is `NULL`.
* `influencer.locationId` matches `LOCATION_ID`.
* `influencer.ig_user_id` is not `NULL`.
* A matching `ig_user` record exists.

The relationship used by the query is:

```text
influencer.ig_user_id → ig_user.id
```

The query also retrieves:

* Influencer ID
* Location ID
* Instagram user ID
* Identity ID
* Follower count

## Post Selection

Posts are matched to an influencer using:

```text
influencer.ig_user_id → ig_post.ig_user_id
```

For every influencer, posts are:

1. Filtered using `POST_DATE_FROM`.
2. Filtered through the entire `POST_DATE_UNTIL` day.
3. Ordered by `ig_post.postDate` from newest to oldest.
4. Limited using `LIMIT_PER_INFLUENCER`.

The following fields are retrieved from MySQL:

* `ig_user_id`
* `ig_post_id`
* `content`
* `likeCount`
* `commentCount`
* `postDate`

## MongoDB Document Structure

Each Instagram post is stored using the following structure:

```json
{
  "user_id": "12345",
  "post_id": "987654321",
  "caption": "Example Instagram caption",
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

### Field behavior

* `user_id` is converted to a string.
* `post_id` is converted to a string.
* `identityId` is converted to a string.
* Missing captions become an empty string.
* Missing numerical values become `null`.
* `postDate` is generated from the MySQL Unix timestamp using `FROM_UNIXTIME()`.

Rows missing either `ig_user_id` or `ig_post_id` are ignored.

## MongoDB Upsert Behavior

Documents are matched using:

```json
{
  "user_id": "<Instagram user ID>",
  "post_id": "<Instagram post ID>"
}
```

The script uses MongoDB's `$set` operation:

```json
{
  "$set": {
    "...": "complete document contents"
  }
}
```

This means:

* New posts are inserted.
* Existing posts are updated.
* Re-running the script does not intentionally create duplicate posts.
* Changed likes, comments, captions, follower counts, or other stored values can be updated.

MongoDB operations use unordered bulk writes:

```python
collection.bulk_write(operations, ordered=False)
```

An error in one operation does not require MongoDB to process all other operations in strict order.

## Automatically Created Indexes

The script automatically creates a unique index on the target collection:

```javascript
db.instagram_posts.createIndex(
  {
    user_id: 1,
    post_id: 1
  },
  {
    unique: true
  }
)
```

This prevents multiple documents from using the same `user_id` and `post_id` combination.

The script also creates a unique checkpoint index based on:

```text
locationId
date_from
date_until
limit_per_influencer
influencerId
```

A second checkpoint index supports lookups using:

```text
locationId
date_from
date_until
limit_per_influencer
done
```

The MongoDB user must have permission to create indexes.

## Checkpoint and Resume Behavior

After an influencer is processed completely, a checkpoint document is created or updated.

Example checkpoint:

```json
{
  "locationId": 4,
  "date_from": "2025-01-01",
  "date_until": "2026-07-31",
  "limit_per_influencer": 10000,
  "influencerId": 123,
  "done": true,
  "updated_at": "2026-08-06 14:30:00",
  "posts_processed": 875
}
```

On the next run, the script loads checkpoint documents where `done` is `true` and skips those influencers.

A checkpoint belongs to the combination of:

* Location
* Start date
* End date
* Per-influencer limit
* Influencer ID

Changing any of these settings creates a different checkpoint scope:

```text
LOCATION_ID
POST_DATE_FROM
POST_DATE_UNTIL
LIMIT_PER_INFLUENCER
```

Changing `BATCH_SIZE` does not create a new checkpoint scope.

Changing `MAX_DOCS` does not create a new checkpoint scope.

Checkpoint writes are grouped into batches of up to 500 operations.

## `MAX_DOCS` Behavior

`MAX_DOCS` controls the total number of documents prepared during one execution.

Unlimited processing:

```python
MAX_DOCS = 0
```

Limit the run to 5,000 documents:

```python
MAX_DOCS = 5000
```

This is useful for:

* Testing the connection.
* Verifying document structure.
* Running a partial migration.
* Limiting execution time.
* Reducing the impact of an initial deployment.

When `MAX_DOCS` stops processing partway through an influencer, that influencer is not marked complete.

A later run can process that influencer again. Previously inserted documents are matched and updated because the script uses upserts.

## `BATCH_SIZE` Behavior

`BATCH_SIZE` controls how many post upserts are included in each MongoDB bulk write.

Example:

```python
BATCH_SIZE = 500
```

Smaller batches:

* Use less memory.
* Produce more MongoDB requests.
* May be safer on constrained systems.

Larger batches:

* Reduce the number of MongoDB requests.
* May improve throughput.
* Use more memory.
* Can create larger write operations.

A value between `100` and `1000` is generally a reasonable starting point, depending on document size and database capacity.

The script ensures that the effective batch size is at least `1`.

## Caption Encoding Repair

The script attempts to repair mojibake caused by UTF-8 text being decoded incorrectly.

Examples of suspicious sequences include:

```text
Ã
Â
â€™
â€œ
â€”
ðŸ
```

The repair process:

1. Calculates a score based on suspicious sequences and characters.
2. Attempts to reconstruct the original bytes.
3. Decodes the bytes as UTF-8.
4. Keeps the repaired text only when its suspicious-character score improves.
5. Repeats the process up to three times.

Byte captions are first decoded as UTF-8. When that fails, the script falls back to CP1252 with replacement characters.

This process is conservative: it keeps the original value when a proposed conversion does not appear to improve the text.

## Retry Behavior

MySQL retrieval functions use exponential retry handling.

Default settings:

```text
Maximum attempts: 5
Initial delay: 0.5 seconds
Maximum delay: 5 seconds
```

Approximate delay sequence:

```text
0.5 seconds
1 second
2 seconds
4 seconds
```

When an operation fails before the final attempt, the script logs a warning:

```text
[warning] operation failed; retrying: <error>
```

After the final failed attempt, the original exception is raised.

The current retry wrapper is applied to:

* Influencer retrieval from MySQL.
* Post retrieval from MySQL.

MongoDB bulk writes are not currently wrapped with the same retry decorator.

## Database Connection Settings

### MySQL

The MySQL connection uses:

```text
Character set: utf8mb4
Unicode support: enabled
Autocommit: enabled
Dictionary cursor: enabled
Connection timeout: 8 seconds
Read timeout: 60 seconds
Write timeout: 60 seconds
```

A new MySQL connection is opened and closed for each retrieval operation.

### MongoDB

The MongoDB connection uses:

```text
Server selection timeout: 15 seconds
```

The MongoDB client remains open for the duration of the synchronization and is closed in a `finally` block.

## Recommended `.gitignore`

Create a `.gitignore` file:

```gitignore
.env
.venv/
venv/
__pycache__/
*.pyc
*.pyo
*.log
.idea/
.vscode/
```

## Security Notes

* Never commit the `.env` file.
* Never hard-code production credentials in `settings.py`.
* Use a read-only MySQL account when possible.
* Grant the MongoDB account only the required collection and index permissions.
* Use TLS-enabled MongoDB connections.
* Restrict database access by IP address or private network.
* Rotate exposed credentials immediately.
* Avoid printing connection strings or passwords in logs.

## Common Errors

### Missing required variables

```text
Missing required environment variables: CRAWL_DB_USERNAME, MONGO_URI_ATLAS_MYHKSG
```

Confirm that the `.env` file exists and contains the required values.

Also confirm that the script is being run from a directory where `load_dotenv()` can find the file.

### MySQL access denied

```text
pymysql.err.OperationalError: (1045, "Access denied...")
```

Check:

* Username
* Password
* Host
* User permissions
* Allowed client IP address

### MySQL connection refused

Check:

* The MySQL server is running.
* The host is correct.
* The port is correct.
* Firewall rules allow the connection.
* The server accepts remote connections.

### Invalid MySQL port

```text
ValueError: Invalid database port value ...
```

Use an integer:

```dotenv
CRAWL_DB_PORT=3306
```

### MongoDB server selection timeout

```text
pymongo.errors.ServerSelectionTimeoutError
```

Check:

* MongoDB URI
* Username and password
* Atlas IP access list
* Network connectivity
* Cluster status
* DNS configuration

### MongoDB authentication failure

Check that:

* The username and password are correct.
* Special URI characters are URL encoded.
* The MongoDB user has access to the configured database.
* The authentication database is correct.

Characters such as `@`, `:`, `/`, and `#` in passwords must be URL encoded when included in a MongoDB URI.

### Duplicate-key error while creating the unique index

A duplicate-key error can occur when the target collection already contains multiple documents with the same:

```text
user_id + post_id
```

Remove or merge duplicate documents before running the script again.

### Permission error while creating indexes

The MongoDB user needs permission to create indexes on:

* The target post collection.
* The checkpoint collection.

### Missing Python package

```text
ModuleNotFoundError: No module named 'pymongo'
```

Install the dependencies:

```bash
pip install pymysql pymongo python-dotenv
```

## Performance Considerations

The script executes one post query per influencer.

For large locations, performance depends on:

* Number of influencers
* Number of posts per influencer
* MySQL indexes
* Network latency
* MongoDB batch size
* MongoDB cluster capacity

Recommended MySQL indexes may include:

```sql
CREATE INDEX idx_influencer_location_user
ON influencer (locationId, deleted_at, ig_user_id);
```

```sql
CREATE INDEX idx_ig_post_user_date
ON ig_post (ig_user_id, postDate);
```

```sql
CREATE INDEX idx_ig_post_id
ON ig_post (ig_post_id);
```

Review existing indexes before creating new ones. Equivalent or overlapping indexes may already exist.

Use MySQL's `EXPLAIN` command to inspect query plans before making production index changes.

## Example `settings.py`

```python
import os

from dotenv import load_dotenv

load_dotenv()

# Post settings
LOCATION_ID = 4
POST_DATE_FROM = "2025-01-01"
POST_DATE_UNTIL = "2026-07-31"
LIMIT_PER_INFLUENCER = 10000
MAX_DOCS = 0
BATCH_SIZE = 500

# MySQL settings
DB_HOST = (
    os.getenv("CRAWL_DB_HOST")
    or os.getenv("CRAWL_DB_READ_HOST")
    or os.getenv("crawl_db_hostname")
    or "localhost"
)

DB_PORT = os.getenv("CRAWL_DB_PORT") or os.getenv("db_port") or "3306"

DB_USERNAME = (
    os.getenv("CRAWL_DB_USERNAME")
    or os.getenv("crawl_db_username")
    or ""
)

DB_PASSWORD = (
    os.getenv("CRAWL_DB_PASSWORD")
    or os.getenv("crawl_db_password")
    or ""
)

DB_DATABASE = (
    os.getenv("CRAWL_DB_DATABASE")
    or os.getenv("crawl_db_name")
    or ""
)

# MongoDB settings
MONGO_URI = (os.getenv("MONGO_URI_ATLAS_MYHKSG") or "").strip()
MONGO_DATABASE = (os.getenv("MONGO_DB_ATLAS_MYHKSG") or "").strip()
MONGO_COLLECTION = (os.getenv("MONGO_COLL_ATLAS_MY") or "").strip()

MONGO_CHECKPOINT_COLL_MYECTION = (
    os.getenv("MONGO_CHECKPOINT_COLL_MY") or "ig-post-my-checkpoint"
).strip()


def get_db_port() -> int:
    try:
        return int(DB_PORT)
    except ValueError as exc:
        raise ValueError(
            f"Invalid database port value {DB_PORT!r}. "
            "Use an integer like '3306'."
        ) from exc
```

## Production Checklist

Before running the full synchronization:

* Confirm the correct `LOCATION_ID`.
* Verify the start and end dates.
* Test with a small `MAX_DOCS` value.
* Confirm the MongoDB target collection.
* Confirm the checkpoint collection.
* Check available MongoDB storage.
* Check MySQL and MongoDB permissions.
* Review indexes.
* Back up important existing data.
* Run the script and inspect sample documents.
* Set `MAX_DOCS = 0` only after testing succeeds.

Example test configuration:

```python
MAX_DOCS = 100
BATCH_SIZE = 50
```

Full run configuration:

```python
MAX_DOCS = 0
BATCH_SIZE = 500
```

## License

Internal project use.
