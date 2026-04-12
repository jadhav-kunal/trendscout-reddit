from neo4j import GraphDatabase
import json
from tqdm import tqdm

# -------------------------
# CONFIG
# -------------------------

NEO4J_URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "reddit123kg"

INPUT_FILE = "data/processed/reddit_tagged.jsonl"

# -------------------------
# DRIVER
# -------------------------

driver = GraphDatabase.driver(NEO4J_URI, auth=(USERNAME, PASSWORD))


# -------------------------
# CREATE GRAPH
# -------------------------

def create_graph(tx, chunk):
    text = chunk["text"]
    tags = chunk.get("tags", [])
    chunk_id = chunk["chunk_id"]
    importance = chunk["importance"]

    # Create chunk node
    tx.run("""
        MERGE (c:Chunk {id: $chunk_id})
        SET c.text = $text,
            c.importance = $importance
    """, chunk_id=chunk_id, text=text, importance=importance)

    # Create tag relationships
    for tag in tags:
        tx.run("""
            MERGE (t:Tag {name: $tag})
            MERGE (c:Chunk {id: $chunk_id})
            MERGE (c)-[:HAS_TAG]->(t)
        """, tag=tag, chunk_id=chunk_id)


def load_data():
    with driver.session() as session:
        with open(INPUT_FILE, "r") as f:
            for line in tqdm(f):
                chunk = json.loads(line)
                session.write_transaction(create_graph, chunk)


if __name__ == "__main__":
    print("Building Knowledge Graph...")
    load_data()
    print("Done!")