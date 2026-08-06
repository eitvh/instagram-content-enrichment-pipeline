# Instagram Content Enrichment Pipeline

A batch-processing pipeline that enriches Instagram post captions with
structured metadata and Gemini embeddings for semantic and vector search.

## Features

- Reads Instagram posts from MongoDB Atlas
- Normalizes caption text
- Extracts tags, brands, mentions, hashtags, sponsorship status, and categories
- Generates Gemini embeddings
- Updates MongoDB documents
- Supports concurrent workers, batching, retries, checkpoints, and cost tracking

## Installation

```bash
python -m venv .venv