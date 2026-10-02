# New influencer post import ? step 2

Run from the repository root:

```powershell
.venv/Scripts/python.exe ig-post-pipeline-new-10k-influenccer/2-get-post/x-get-inf-post.py
```

Set `LOCATION_ID` in `settings.py`: `1` = HK (default), `3` = MY, `4` = SG. Run separately for each location.

Only active influencers with `sourceFrom = EXTERNAL_HELPER` and an Instagram user are selected. Posts include January 1, 2025 through all of July 31, 2026, using the existing MySQL session timezone behavior. Both SQL queries check the influencer source.

Credentials load from the root `.env`, using existing MySQL variables, `MONGO_URI_ATLAS_MYHKSG`, and `MONGO_DB_ATLAS_MYHKSG`.

| Country | Main post documents | Checkpoints |
| --- | --- | --- |
| MY | `MONGO_COLL_NEW_INFLUENCER_MY=ig-my-new-10k-influencer` | `MONGO_CHECKPOINT_NEW_POST_MY=ig-my-new-10k-checkpoint` |
| HK | `MONGO_COLL_NEW_INFLUENCER_HK=ig-hk-new-10k-influencer` | `MONGO_CHECKPOINT_NEW_POST_HK=ig-hk-new-10k-checkpoint` |
| SG | `MONGO_COLL_NEW_INFLUENCER_SG=ig-sg-new-10k-influencer` | `MONGO_CHECKPOINT_NEW_POST_SG=ig-sg-new-10k-checkpoint` |

Despite the collection names, main collections store post documents in the original step 2 format, with influencer metadata, upserted by `(user_id, post_id)`. No separate influencer-only documents are written. `MONGO_COLL_NEW_POST_*` variables are not used.

Checkpoint resume, caption repair, retries, batching, and progress follow the original step 2. Defaults retain 10,000 posts per influencer, 10,000,000 posts per run, and batches of 500. These are post limits, not follower filters. Set `MAX_DOCS = 0` for no run-wide post limit.

The original pipeline and step 3 are unchanged.

## Run all countries

```bash
.venv/Scripts/python.exe ig-post-pipeline-new-10k-influencer/2-get-post/x-get-inf-post.py --all-countries
```

Runs MY, HK, then SG sequentially with their respective main and checkpoint collections. All country settings are validated before database work starts. `MAX_DOCS` applies separately to each country. A failure stops the run; rerun the command to resume using each country's checkpoints. Without the flag, only `LOCATION_ID` runs.
