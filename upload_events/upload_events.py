import os
from uuid import UUID
from datetime import datetime, UTC
from qdrant_client import QdrantClient
from tqdm import tqdm
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from qdrant_client.http.models import PointIdsList

def main():
    VECTOR_NAME = "dense"

    # Load .env config
    load_dotenv()

    # Setup Qdrant
    client = QdrantClient(
        url=os.getenv("QDRANT_URL"),
        api_key=os.getenv("QDRANT_API_KEY")
    )
    client.set_model("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
    collection_name = "events"

    # def load_last_sync_time_qdrant():
    #     try:
    #         result = client.retrieve(
    #             collection_name=collection_name,
    #             ids=[0]
    #         )
    #         if result and result[0].payload.get("last_sync_time"):
    #             return result[0].payload["last_sync_time"]
    #     except Exception:
    #         pass
    #     return "2000-01-01 00:00:00"

    # def save_sync_time_qdrant():
    #     now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    #     client.upsert(
    #         collection_name=collection_name,
    #         points=[
    #             {
    #                 "id": 0, 
    #                 "vector": {VECTOR_NAME: [0.0] * 384},
    #                 "payload": {
    #                     "sync_marker": True,
    #                     "last_sync_time": now
    #                 }
    #             }
    #         ]
    #     )

    # last_sync_time = load_last_sync_time_qdrant()

    # PostgreSQL connection
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB")
    )
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    # Get updated or new events
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
    """)

    rows = cursor.fetchall()

    # Also fetch ticket type and showtime info for all relevant events
    event_ids = tuple(row["id"] for row in rows)
    ticket_data = {}
    showtime_data = {}

    if event_ids:
        # Get ticket prices & free status
        cursor.execute("""
            SELECT event_id, is_free, price
            FROM ticket_types
            WHERE event_id IN %s;
        """, (event_ids,))
        for ticket in cursor.fetchall():
            event_id = ticket["event_id"]
            ticket_data.setdefault(event_id, []).append(ticket)

        # Get showtimes
        cursor.execute("""
            SELECT event_id, start_time
            FROM shows
            WHERE event_id IN %s;
        """, (event_ids,))
        for show in cursor.fetchall():
            event_id = show["event_id"]
            showtime_data.setdefault(event_id, []).append(show["start_time"])

    conn.close()

    # Ensure collection exists
    if not client.collection_exists(collection_name):
        client.create_collection(
            collection_name=collection_name,
            vectors_config=client.get_fastembed_vector_params(),
            sparse_vectors_config=client.get_fastembed_sparse_vector_params(),
        )

    # ✅ Ensure payload indexes exist (for optimized search filters and BM25)
    try:
        index_fields = [
            ("categories", "keyword"),
            ("city", "keyword"),
            ("startTime", "float"),
            ("text", "text")  # ✅ For BM25 search support
        ]
        for field_name, schema in index_fields:
            try:
                client.create_payload_index(
                    collection_name=collection_name,
                    field_name=field_name,
                    field_schema=schema
                )
                print(f"Index created for '{field_name}' ({schema})")
            except Exception as e:
                print(f"Index for '{field_name}' might already exist or failed: {e}")
    except Exception as e:
        print(f"Failed to create payload indexes: {e}")

    # Fetch existing IDs in Qdrant
    existing_qdrant_ids = set()
    scroll = client.scroll(collection_name=collection_name, limit=1000)
    existing_qdrant_ids.update(p.id for p in scroll[0])

    # Determine deleted IDs
    db_ids = set(row["id"] for row in rows)
    deleted_ids = list(existing_qdrant_ids - db_ids)

    # Delete removed events
    if deleted_ids:
        client.delete(
            collection_name=collection_name,
            points_selector=PointIdsList(points=deleted_ids)
        )
        print(f"Deleted {len(deleted_ids)} events from Qdrant (no longer in DB).")

    # Prepare data for upserting
    documents = []
    metadata = []
    ids = []

    def snake_to_camel(s):
        parts = s.split('_')
        return parts[0] + ''.join(word.capitalize() for word in parts[1:])

    def dict_keys_to_camel_case(d):
        if isinstance(d, dict):
            return {snake_to_camel(k): dict_keys_to_camel_case(v) for k, v in d.items()}
        elif isinstance(d, list):
            return [dict_keys_to_camel_case(i) for i in d]
        else:
            return d

    i = 0
    for row in rows:
        event_id = row["id"]
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

        # Calculate lowest price
        tickets = ticket_data.get(event_id, [])
        has_free_ticket = any(t["is_free"] for t in tickets)
        lowest_price = 0 if has_free_ticket else (
            min((t["price"] for t in tickets if t["price"] is not None), default=None)
        )

        # Calculate soonest start_time
        start_times = showtime_data.get(event_id, [])
        soonest_time = min(start_times) if start_times else None
        soonest_time_float = soonest_time.timestamp() if soonest_time else None

        i += 1
        print(i, start_times, soonest_time_float)
        print(i, lowest_price)

        documents.append(text)
        meta = {
            "id": event_id,
            "eventName": row["event_name"],
            "eventDescription": row.get("event_description", ""),
            "city": (row.get("city_name_en") or row.get("city_name") or "").lower(),
            "district": row.get("district_name_en") or row.get("district_name"),
            "ward": row.get("ward_name_en") or row.get("ward_name"),
            "street": row.get("street"),
            "categories": [cat.lower() for cat in row.get("categories", [])],
            "eventLogoUrl": row.get("event_logo_url"),
            "minimumPrice": lowest_price,
            "startTime": soonest_time_float,
            "text": text  # ✅ For BM25 search
        }
        meta_camel = dict_keys_to_camel_case(meta)
        metadata.append(meta_camel)
        ids.append(event_id)

    # Upsert events
    if documents:
        client.add(
            collection_name=collection_name,
            documents=documents,
            metadata=metadata,
            ids=tqdm(ids),
        )
        print(f"Upserted {len(documents)} events into Qdrant.")

if __name__ == "__main__":
    main()
