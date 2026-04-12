import json
import re
import math
import logging
import os
from tqdm import tqdm

INPUT_FILE = "data/raw/reddit_posts.jsonl"
OUTPUT_FILE = "data/processed/reddit_cleaned.jsonl"

MIN_SCORE = 1
MIN_TEXT_LENGTH = 20

logging.basicConfig(level=logging.INFO)

# -------------------------
# TEXT CLEANING
# -------------------------

def normalize_text(text):
    if not text:
        return ""

    text = re.sub(r"http\S+", "", text)
    text = re.sub(r"\[.*?\]\(.*?\)", "", text)
    text = re.sub(r"\s+", " ", text)

    return text.strip()


def calculate_importance(score, comments):
    return math.log(1 + score) + 0.5 * math.log(1 + comments)


def is_valid_record(text, score, author):
    if not text or len(text) < MIN_TEXT_LENGTH:
        return False
    if score < MIN_SCORE:
        return False
    if author == "AutoModerator":
        return False
    return True


# -------------------------
# PROCESSING
# -------------------------

def process_data():
    logging.info("Cleaning and filtering Reddit data...")

    os.makedirs("data/processed", exist_ok=True)

    with open(INPUT_FILE, "r") as infile, open(OUTPUT_FILE, "w") as outfile:
        for line in tqdm(infile):
            post = json.loads(line)

            post_id = post["post_id"]

            # ---- Process Post ----
            text = normalize_text(post.get("title", "") + " " + post.get("selftext", ""))
            score = post.get("score", 0)
            author = post.get("author")

            if is_valid_record(text, score, author):
                record = {
                    "id": post_id,
                    "type": "post",
                    "text": text,
                    "score": score,
                    "importance": calculate_importance(score, post.get("num_comments", 0))
                }
                outfile.write(json.dumps(record) + "\n")

            # ---- Process Comments ----
            for comment in post.get("comments", []):
                c_text = normalize_text(comment.get("body", ""))
                c_score = comment.get("score", 0)
                c_author = comment.get("author")

                if is_valid_record(c_text, c_score, c_author):
                    record = {
                        "id": comment.get("id"),
                        "type": "comment",
                        "post_id": post_id,
                        "text": c_text,
                        "score": c_score,
                        "importance": calculate_importance(c_score, 0)
                    }
                    outfile.write(json.dumps(record) + "\n")

    logging.info("Cleaning complete!")


if __name__ == "__main__":
    process_data()