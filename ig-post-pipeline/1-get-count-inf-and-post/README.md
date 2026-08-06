# Instagram Post Count Utility

A Python command-line utility that counts:

1. The number of active influencers associated with a specific location.
2. The number of unique Instagram posts belonging to those influencers within a configured date range.

The script queries a MySQL database and applies a maximum post limit separately to each influencer.

## Features

* Retrieves active influencers for a configured location.
* Includes only influencers connected to an Instagram user.
* Filters Instagram posts by date.
* Counts unique posts using `ig_post_id`.
* Applies a configurable post limit per influencer.
* Automatically retries failed database operations.
* Prints progress information while processing.
* Loads database credentials from environment variables or a `.env` file.


## Requirements

* Python 3.8 or newer
* MySQL-compatible database
* Access to the following database tables:

  * `influencer`
  * `ig_user`
  * `ig_post`

## Installation

Create and activate a virtual environment:

```bash
python -m venv .venv
```

On macOS or Linux:

```bash
source .venv/bin/activate
```

On Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

Install the required packages:

```bash
pip install pymysql python-dotenv
```

Alternatively, create a `requirements.txt` file:

```text
PyMySQL
python-dotenv
```

Then install it with:

```bash
pip install -r requirements.txt
```

## Configuration

The script reads its general settings from `settings.py`.

```python
# Post settings
LOCATION_ID = 4
POST_DATE_FROM = "2025-01-01"
POST_DATE_UNTIL = "2026-07-31"
LIMIT_PER_INFLUENCER = 10000
```

### Post settings

| Setting                | Description                                          |
| ---------------------- | ---------------------------------------------------- |
| `LOCATION_ID`          | The location ID used to select influencers.          |
| `POST_DATE_FROM`       | The earliest post date to include.                   |
| `POST_DATE_UNTIL`      | The latest post date to include.                     |
| `LIMIT_PER_INFLUENCER` | Maximum number of posts counted for each influencer. |

Dates should normally use the following format:

```text
YYYY-MM-DD
```

The script also supports dates written as:

```text
DD/MM/YYYY
```

For example, `31/07/2026` is normalized to `2026-07-31`.

Set either date value to `None` or an empty string to disable that date boundary:

```python
POST_DATE_FROM = None
POST_DATE_UNTIL = None
```

## Database Environment Variables

Create a `.env` file in the project directory:

```dotenv
CRAWL_DB_HOST=localhost
CRAWL_DB_PORT=3306
CRAWL_DB_USERNAME=my_database_user
CRAWL_DB_PASSWORD=my_database_password
CRAWL_DB_DATABASE=my_database_name
```

The following environment variable names are supported:

| Preferred variable  | Alternative variable |
| ------------------- | -------------------- |
| `CRAWL_DB_HOST`     | `crawl_db_hostname`  |
| `CRAWL_DB_PORT`     | `db_port`            |
| `CRAWL_DB_USERNAME` | `crawl_db_username`  |
| `CRAWL_DB_PASSWORD` | `crawl_db_password`  |
| `CRAWL_DB_DATABASE` | `crawl_db_name`      |

When no host or port is provided, the defaults are:

```text
Host: localhost
Port: 3306
```

Do not commit the `.env` file to source control. Add it to `.gitignore`:

```gitignore
.env
.venv/
__pycache__/
*.pyc
```

## Usage

Run the main script:

```bash
python count_posts.py
```

Replace `count_posts.py` with the actual name of the file containing the `main()` function.

## Output

Progress and configuration details are written to standard error:

```text
[info] locationId=4 influencers(with ig_user)=125 from=2025-01-01 until=2026-07-31 limit_per_influencer=10000
[progress] 1/125 total_posts_so_far=43
[progress] 10/125 total_posts_so_far=512
[progress] 125/125 total_posts_so_far=6842
```

The final results are written to standard output as two separate lines:

```text
125
6842
```

The values represent:

```text
Line 1: Number of influencers
Line 2: Total number of posts
```

This output format makes it easy to capture the results in another script:

```bash
python count_posts.py > results.txt
```

Progress messages will still appear in the terminal because they are written to standard error.

To capture both the results and progress messages:

```bash
python count_posts.py > results.txt 2> progress.log
```

## Query Behavior

### Influencer selection

An influencer is included when all of the following conditions are met:

* `influencer.deleted_at` is `NULL`.
* `influencer.locationId` matches `LOCATION_ID`.
* `influencer.ig_user_id` is not `NULL`.
* The related `ig_user` record exists.

The relevant relationship is:

```text
influencer.ig_user_id → ig_user.id
```

### Post selection

For every selected influencer, the script:

1. Joins `ig_post` to `influencer` through `ig_user_id`.
2. Applies the configured date range.
3. Groups rows by `ig_post.ig_post_id`.
4. Orders posts by `ig_post.postDate` in descending order.
5. Limits the result to `LIMIT_PER_INFLUENCER`.
6. Counts the limited set of unique posts.

The relevant relationship is:

```text
influencer.ig_user_id → ig_post.ig_user_id
```

## Date Filtering

The `ig_post.postDate` column is expected to contain Unix timestamps.

The script converts configured dates using MySQL's `UNIX_TIMESTAMP()` function:

```sql
ig_post.postDate >= UNIX_TIMESTAMP(%s)
```

```sql
ig_post.postDate <= UNIX_TIMESTAMP(%s)
```

The upper date boundary is inclusive. However, when a date is provided without a time, MySQL normally interprets it as midnight at the beginning of that date.

For example:

```text
2026-07-31
```

may be treated as:

```text
2026-07-31 00:00:00
```

This can exclude posts published later on July 31. To include the entire final day, provide an explicit end time if supported by the database configuration:

```python
POST_DATE_UNTIL = "2026-07-31 23:59:59"
```

## Retry Behavior

The database functions are wrapped with an exponential retry mechanism.

Default retry settings:

```text
Maximum attempts: 5
Initial delay: 0.5 seconds
Maximum delay: 5 seconds
```

The delays progress approximately as follows:

```text
0.5 seconds
1 second
2 seconds
4 seconds
```

If the final attempt fails, the original exception is raised and the program exits with an error.

## Database Connections

The script opens a separate database connection for:

* Fetching the influencer list.
* Counting posts for each influencer.

All connections are closed in a `finally` block, including when a query fails.

The connection uses:

```text
Character set: utf8mb4
Cursor type: dictionary cursor
Autocommit: enabled
Connection timeout: 8 seconds
```

The post-count query also executes:

```sql
SET NAMES latin1
```

Make sure this character-set override is required for the target database. Remove it if the database and tables use `utf8mb4` consistently.

## Performance Considerations

The script executes one post-count query per influencer. For a large number of influencers, this can result in many database connections and queries.

Recommended indexes include:

```sql
CREATE INDEX idx_influencer_location
ON influencer (locationId, deleted_at, ig_user_id);
```

```sql
CREATE INDEX idx_ig_post_user_date
ON ig_post (ig_user_id, postDate);
```

```sql
CREATE INDEX idx_ig_post_post_id
ON ig_post (ig_post_id);
```

Review the existing indexes before creating new ones, because equivalent indexes may already exist.

## Common Errors

### Access denied

```text
pymysql.err.OperationalError: (1045, "Access denied...")
```

Check:

* Database username
* Database password
* User permissions
* Allowed database host

### Unknown database

```text
pymysql.err.OperationalError: (1049, "Unknown database...")
```

Confirm that `CRAWL_DB_DATABASE` contains the correct database name.

### Connection refused

Check:

* The database server is running.
* The host and port are correct.
* Firewall rules permit the connection.
* The database accepts remote connections when applicable.

### Invalid port

```text
ValueError: Invalid database port value ...
```

Set the port to an integer:

```dotenv
CRAWL_DB_PORT=3306
```

### Missing module

```text
ModuleNotFoundError: No module named 'pymysql'
```

Install the dependencies:

```bash
pip install pymysql python-dotenv
```

## Security Notes

* Never hard-code production database passwords in `settings.py`.
* Store credentials in environment variables or a local `.env` file.
* Do not commit `.env` files to Git.
* Use a database account with only the permissions required to read the relevant tables.
* Avoid printing credentials in logs or error messages.

## Example Settings

```python
import os

from dotenv import load_dotenv

load_dotenv()

# Post settings
LOCATION_ID = 4
POST_DATE_FROM = "2025-01-01"
POST_DATE_UNTIL = "2026-07-31 23:59:59"
LIMIT_PER_INFLUENCER = 10000

# Database settings
DB_HOST = os.getenv("CRAWL_DB_HOST") or "localhost"
DB_PORT = os.getenv("CRAWL_DB_PORT") or "3306"
DB_USERNAME = os.getenv("CRAWL_DB_USERNAME") or ""
DB_PASSWORD = os.getenv("CRAWL_DB_PASSWORD") or ""
DB_DATABASE = os.getenv("CRAWL_DB_DATABASE") or ""


def get_db_port() -> int:
    try:
        return int(DB_PORT)
    except ValueError as exc:
        raise ValueError(
            f"Invalid database port value {DB_PORT!r}. "
            "Use an integer like '3306'."
        ) from exc
```

## License

Internal project use.
