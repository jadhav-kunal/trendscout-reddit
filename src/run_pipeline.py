import subprocess
import argparse
import logging
import os

logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser()
parser.add_argument("--subreddit", required=True, help="Subreddit name")
args = parser.parse_args()

SUBREDDIT = args.subreddit

# -------------------------
# CLEAN OLD FILES
# -------------------------

files_to_clear = [
    f"data/raw/{SUBREDDIT}_posts.jsonl",
    f"data/processed/{SUBREDDIT}_cleaned.jsonl",
    f"data/chunks/{SUBREDDIT}_chunks.jsonl",
    f"data/processed/{SUBREDDIT}_tagged.jsonl"
]

for file in files_to_clear:
    if os.path.exists(file):
        os.remove(file)

# -------------------------
# PIPELINE STEPS
# -------------------------

steps = [
    ("Scraping", f"python3 src/scraper.py --subreddit {SUBREDDIT}"),
    ("Cleaning", f"python3 src/clean_data.py --subreddit {SUBREDDIT}"),
    ("Chunking", f"python3 src/chunk_data.py --subreddit {SUBREDDIT}"),
    ("Tagging", f"python3 src/tag_data.py --subreddit {SUBREDDIT}"),
    ("Building KG", f"python3 src/build_kg.py --subreddit {SUBREDDIT}")
]

# -------------------------
# RUN PIPELINE
# -------------------------

def run_pipeline():
    logging.info(f"Running pipeline for r/{SUBREDDIT}")

    for step_name, command in steps:
        logging.info(f">> {step_name}...")
        result = subprocess.run(command, shell=True)

        if result.returncode != 0:
            logging.error(f"Failed at: {step_name}")
            break

    logging.info("Pipeline completed!!!")


if __name__ == "__main__":
    run_pipeline()