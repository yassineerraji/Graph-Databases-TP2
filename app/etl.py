import os, time
import psycopg2
from neo4j import GraphDatabase
from pathlib import Path

PG_DSN = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@postgres:5432/shop")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# --- Helpers ---
def run_cypher(query, params=None):
    with driver.session() as session:
        return session.run(query, params or {}).data()

def run_cypher_file_schema_only(path):
    """
    Execute only schema-related statements (constraints/indexes) from a cypher file.
    Skips Browser directives (e.g., ::param) and non-schema queries.
    """
    text = Path(path).read_text()
    statements = []
    buffer = []
    for line in text.splitlines():
        stripped = line.strip()
        # Skip browser directives or param declarations
        if stripped.startswith(":"):
            continue
        buffer.append(line)
        if ";" in line:
            statements.append("\n".join(buffer))
            buffer = []

    schema_keywords = ("CREATE CONSTRAINT", "DROP CONSTRAINT", "CREATE INDEX")
    with driver.session() as session:
        for raw in statements:
            stmt = raw.strip()
            if not stmt:
                continue
            upper = stmt.upper()
            if any(k in upper for k in schema_keywords):
                session.run(stmt)

def chunk(data, size=100):
    for i in range(0, len(data), size):
        yield data[i:i+size]

def wait_for_postgres():
    import psycopg2
    while True:
        try:
            conn = psycopg2.connect(PG_DSN)
            conn.close()
            print("✅ Postgres ready")
            break
        except Exception as e:
            print("⏳ Waiting for Postgres...", e)
            time.sleep(2)

def wait_for_neo4j():
    while True:
        try:
            run_cypher("RETURN 1")
            print("✅ Neo4j ready")
            break
        except Exception as e:
            print("⏳ Waiting for Neo4j...", e)
            time.sleep(2)

# --- Main ETL ---
def etl():
    """
    Main ETL function that migrates data from PostgresQL to Neo4j.
    """
    wait_for_postgres()
    wait_for_neo4j()

    # Step 1: Load Neo4j schema from queries.cypher (constraints/indexes only)
    queries_path = Path(__file__).with_name("queries.cypher")
    if queries_path.exists():
        run_cypher_file_schema_only(queries_path)

    # Step 2: Extract from Postgres
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()
    cur.execute("SELECT id, name, join_date FROM customers;")
    customers = cur.fetchall()
    cur.execute("SELECT id, name FROM categories;")
    categories = cur.fetchall()
    cur.execute("SELECT id, name, price, category_id FROM products;")
    products = cur.fetchall()
    cur.execute("SELECT id, customer_id, ts FROM orders;")
    orders = cur.fetchall()
    cur.execute("SELECT order_id, product_id, quantity FROM order_items;")
    order_items = cur.fetchall()
    cur.execute("SELECT id, customer_id, product_id, event_type, ts FROM events;")
    events = cur.fetchall()
    cur.close()
    conn.close()

    # Step 3: Load into Neo4j
    with driver.session() as session:
        # Create Customers
        session.run("""
        UNWIND $rows AS r
        MERGE (:Customer {id:r.id, name:r.name, join_date:r.join_date})
        """, rows=[{"id": c[0], "name": c[1], "join_date": str(c[2])} for c in customers])

        # Create Categories
        session.run("""
        UNWIND $rows AS r
        MERGE (:Category {id:r.id, name:r.name})
        """, rows=[{"id": c[0], "name": c[1]} for c in categories])

        # Create Products + link to Category
        session.run("""
        UNWIND $rows AS r
        MERGE (p:Product {id:r.id})
        SET p.name = r.name, p.price = r.price
        WITH p, r
        MATCH (c:Category {id:r.category_id})
        MERGE (p)-[:IN_CATEGORY]->(c)
        """, rows=[{"id": p[0], "name": p[1], "price": float(p[2]), "category_id": p[3]} for p in products])

        # Create Orders
        session.run("""
        UNWIND $rows AS r
        MERGE (o:Order {id:r.id, ts:r.ts})
        WITH o, r
        MATCH (c:Customer {id:r.customer_id})
        MERGE (c)-[:PLACED]->(o)
        """, rows=[{"id": o[0], "customer_id": o[1], "ts": str(o[2])} for o in orders])

        # Order -> Product
        session.run("""
        UNWIND $rows AS r
        MATCH (o:Order {id:r.order_id})
        MATCH (p:Product {id:r.product_id})
        MERGE (o)-[rel:CONTAINS]->(p)
        SET rel.quantity = r.quantity
        """, rows=[{"order_id": oi[0], "product_id": oi[1], "quantity": oi[2]} for oi in order_items])

        # Event relationships
        session.run("""
        UNWIND $rows AS r
        MATCH (c:Customer {id:r.customer_id})
        MATCH (p:Product {id:r.product_id})
        CALL apoc.create.relationship(c, r.event_type, {ts:r.ts, id:r.id}, p) YIELD rel
        RETURN count(*)
        """, rows=[{"id": e[0], "customer_id": e[1], "product_id": e[2], "event_type": e[3].upper(), "ts": str(e[4])} for e in events])

    print("ETL done.")

if __name__ == "__main__":
    etl()