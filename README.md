# TrendScout AI - Reddit Data Pipeline

This module handles Reddit data ingestion for TrendScout AI.

- Scrapes subreddit data using Reddit JSON endpoints
- Extracts posts + comments
- Stores data in JSONL format
- Cleaning & filtering
- Chunking
- Knowledge graph construction

## How to Run the Project


1. Install Dependencies
    
    `pip3 install -r requirements.txt`

2. Run the Pipeline

    `python3 src/run_pipeline.py --subreddit startups`




### Output Files

After execution, the pipeline will generate:

`data/raw/startups_posts.jsonl`

`data/processed/startups_cleaned.jsonl`

`data/chunks/startups_chunks.jsonl`

`data/processed/startups_tagged.jsonl`

## Notes

- Make sure Neo4j is running before executing the pipeline (for knowledge graph step).
- The pipeline overwrites previous outputs to avoid duplication.
- You can replace `startups` with any subreddit of your choice. Example: `Entrepreneur`.