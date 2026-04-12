import json
import logging
import os
from tqdm import tqdm

INPUT_FILE = "data/chunks/reddit_chunks.jsonl"
OUTPUT_FILE = "data/processed/reddit_tagged.jsonl"

logging.basicConfig(level=logging.INFO)

# -------------------------
# TAG DEFINITIONS
# -------------------------

TAG_RULES = {
    "FUNDING": ["funding", "raised", "seed", "series", "investment"],
    "HIRING": ["hiring", "job", "engineer", "recruit", "salary"],
    "TECH": ["ai", "llm", "model", "api", "saas", "ml"],
    "MARKETING": ["growth", "seo", "users", "acquisition", "traffic"],
    "PRODUCT": ["feature", "mvp", "build", "launch", "prototype"]
}

# -------------------------
# TAGGING FUNCTION
# -------------------------

def assign_tags(text):
    text_lower = text.lower()
    tags = []

    for tag, keywords in TAG_RULES.items():
        for word in keywords:
            if word in text_lower:
                tags.append(tag)
                break

    return list(set(tags))


# -------------------------
# PROCESS PIPELINE
# -------------------------

def tag_data():
    logging.info("Tagging chunks with baseline NLP...")

    os.makedirs("data/processed", exist_ok=True)

    total = 0

    with open(INPUT_FILE, "r") as infile, open(OUTPUT_FILE, "w") as outfile:
        for line in tqdm(infile):
            chunk = json.loads(line)

            tags = assign_tags(chunk["text"])

            chunk["tags"] = tags

            outfile.write(json.dumps(chunk) + "\n")
            total += 1

    logging.info(f"Tagging complete. Processed: {total} !")


# -------------------------
# RUN
# -------------------------

if __name__ == "__main__":
    tag_data()