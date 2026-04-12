import requests
import time
import json
import logging
from datetime import datetime
from tqdm import tqdm
import os

# =========================
# CONFIG
# =========================

SUBREDDIT = "startups"  # change this
SORT = "new"            # new / hot
LIMIT = 25              # posts per request
OUTPUT_FILE = "reddit_data.jsonl"

HEADERS = {
    "User-Agent": "TrendScoutAI/1.0 (Academic Research)"
}

# =========================
# LOGGING SETUP
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# =========================
# HELPER FUNCTIONS
# =========================

def fetch_posts(subreddit, sort="new"):
    url = f"https://www.reddit.com/r/{subreddit}/{sort}/.json?limit={LIMIT}"
    
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Error fetching posts: {e}")
        return None


def fetch_comments(permalink):
    url = f"https://www.reddit.com{permalink}.json"
    
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Error fetching comments: {e}")
        return None


def extract_comments(comment_data):
    comments = []

    def recurse(children):
        for child in children:
            if child["kind"] != "t1":
                continue

            data = child["data"]

            comments.append({
                "id": data.get("id"),
                "author": data.get("author"),
                "body": data.get("body"),
                "score": data.get("score"),
                "created_utc": data.get("created_utc")
            })

            # nested replies
            if data.get("replies"):
                if isinstance(data["replies"], dict):
                    recurse(data["replies"]["data"]["children"])

    recurse(comment_data)
    return comments


def save_jsonl(file_path, data):
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(data) + "\n")


# =========================
# MAIN PIPELINE
# =========================

def scrape_subreddit(subreddit):
    logging.info(f"🚀 Starting scrape for r/{subreddit}")

    posts_json = fetch_posts(subreddit, SORT)
    
    if not posts_json:
        logging.error("No data received.")
        return

    posts = posts_json["data"]["children"]

    for post_wrapper in tqdm(posts, desc=f"Scraping r/{subreddit}"):
        post = post_wrapper["data"]

        post_data = {
            "post_id": post.get("id"),
            "title": post.get("title"),
            "selftext": post.get("selftext"),
            "author": post.get("author"),
            "score": post.get("score"),
            "num_comments": post.get("num_comments"),
            "created_utc": post.get("created_utc"),
            "permalink": post.get("permalink"),
            "fetched_at": int(time.time())
        }

        # Fetch comments
        comments_json = fetch_comments(post_data["permalink"])

        if comments_json and len(comments_json) > 1:
            comment_tree = comments_json[1]["data"]["children"]
            comments = extract_comments(comment_tree)
        else:
            comments = []

        post_data["comments"] = comments

        # Save
        save_jsonl(OUTPUT_FILE, post_data)

        # Respect rate limit
        time.sleep(1)

    logging.info("✅ Scraping completed")


# =========================
# RUN
# =========================

if __name__ == "__main__":
    scrape_subreddit(SUBREDDIT)