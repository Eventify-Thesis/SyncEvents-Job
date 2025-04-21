import os
import json
from datetime import datetime,UTC
from qdrant_client import QdrantClient
from tqdm import tqdm
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load .env config
load_dotenv()

def load_last_sync_time_qdrant():
    try:
        result = client.retrieve(
            collection_name=collection_name,
            ids=["__sync_timestamp__"]
        )
        if result and result[0].payload.get("last_sync_time"):
            return result[0].payload["last_sync_time"]
    except Exception:
        pass
    return "2000-01-01 00:00:00"

def save_sync_time_qdrant():
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    client.upsert(
        collection_name=collection_name,
        points=[
            {
                "id": "__sync_timestamp__",
                "vector": [0.0] * client.get_fastembed_vector_params()["size"],  # dummy vector
                "payload": {
                    "last_sync_time": now
                }
            }
        ]
    )

last_sync_time = load_last_sync_time_qdrant()

# PostgreSQL connection
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    dbname=os.getenv("POSTGRES_DB")
)
cursor = conn.cursor(cursor_factory=RealDictCursor)

# Get all updated or deleted events
cursor.execute("""
    SELECT 
        e.id,
        e.event_name,
        e.event_description,
        e.street,
        e.categories,
        e.updated_at,
    e.event_logo_url,

        c.name AS city_name,
        c.name_en AS city_name_en,
        d.name AS district_name,
        d.name_en AS district_name_en,
        w.name AS ward_name,
        w.name_en AS ward_name_en

    FROM events e
    LEFT JOIN cities c ON (e.city_id)::integer = c.origin_id
    LEFT JOIN districts d ON (e.district_id)::integer = d.origin_id
    LEFT JOIN wards w ON (e.ward_id)::integer = w.origin_id
    WHERE e.updated_at > %s;
""", (last_sync_time,))

rows = cursor.fetchall()
conn.close()

# Setup Qdrant
client = QdrantClient(
    url=os.getenv("QDRANT_URL"),
    api_key=os.getenv("QDRANT_API_KEY")
)
client.set_model("sentence-transformers/all-MiniLM-L6-v2")
client.set_sparse_model("prithivida/Splade_PP_en_v1")
collection_name = "events"

if not client.collection_exists(collection_name):
    client.create_collection(
        collection_name=collection_name,
        vectors_config=client.get_fastembed_vector_params(),
        sparse_vectors_config=client.get_fastembed_sparse_vector_params(),
    )

# Fetch all existing IDs from Qdrant
existing_qdrant_ids = set()
scroll = client.scroll(collection_name=collection_name, limit=1000)
existing_qdrant_ids.update(p.id for p in scroll[0])

# Get updated event rows from DB
updated_rows = rows
db_ids = set(row["id"] for row in updated_rows)

# Find which Qdrant records need to be deleted
deleted_ids = list(existing_qdrant_ids - db_ids)

# Delete removed events
if deleted_ids:
    client.delete(collection_name=collection_name, points=deleted_ids)
    print(f"Deleted {len(deleted_ids)} events from Qdrant (no longer in DB).")

# Prepare and upsert updated/new events
documents = []
metadata = []
ids = []

for row in updated_rows:
    categories = row["categories"] or []
    categories_str = ", ".join(categories)

    location_parts = filter(None, [
        row.get("street"),
        row.get("ward_name"),
        row.get("district_name"),
        row.get("city_name"),
    ])
    location_str = ", ".join(location_parts)

    description = row.get("event_description") or ""
    text = f"{row['event_name']} - {description}. Located at {location_str}. Categories: {categories_str}"

    documents.append(text)
    metadata.append({
        "id": row["id"],
        "eventName": row["event_name"],
        "city": row.get("city_name_en") or row.get("city_name"),
        "district": row.get("district_name_en") or row.get("district_name"),
        "ward": row.get("ward_name_en") or row.get("ward_name"),
        "street": row.get("street"),
        "categories": categories,
        "event_logo_url": row.get("event_logo_url")
    })
    ids.append(row["id"])

# Add or update events in Qdrant
if documents:
    client.add(
        collection_name=collection_name,
        documents=documents,
        metadata=metadata,
        ids=tqdm(ids),
    )
    print(f"Upserted {len(documents)} events into Qdrant.")

# Save sync time
save_sync_time_qdrant()
