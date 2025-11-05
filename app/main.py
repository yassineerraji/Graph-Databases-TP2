import os
from typing import Any, Dict, List
from fastapi import FastAPI
from neo4j import GraphDatabase

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

app = FastAPI(title="TD1 Graph Recommender", version="1.0.0")


@app.get("/health")
def health() -> Dict[str, Any]:
    """
    Lightweight health endpoint to verify the API and Neo4j connectivity.
    Returns {ok: true} if a trivial Cypher query succeeds.
    """
    try:
        with driver.session() as s:
            s.run("RETURN 1 AS ok").single()
        return {"ok": True}
    except Exception:
        return {"ok": False}

@app.get("/recs/{customer_id}")
def recs(customer_id: str, limit: int = 5) -> Dict[str, Any]:
    query = """
    MATCH (c:Customer {id:$cid})-[:PLACED]->(:Order)-[:CONTAINS]->(p:Product)
    MATCH (o2:Order)-[:CONTAINS]->(p)
    MATCH (o2)-[:CONTAINS]->(rec:Product)
    WHERE NOT (c)-[:PLACED]->(:Order)-[:CONTAINS]->(rec)
    WITH rec, count(*) AS score
    RETURN rec.id AS product_id, rec.name AS name, score
    ORDER BY score DESC LIMIT $limit
    """
    with driver.session() as s:
        rows = s.run(query, {"cid": customer_id, "limit": limit}).data()
    return {"customer": customer_id, "recommendations": rows}


@app.get("/recs/product/{product_id}")
def recs_by_product(product_id: str, limit: int = 10) -> Dict[str, Any]:
    """
    Product-to-product co-occurrence based on orders.
    Given a product, find other products frequently bought in the same orders.
    """
    query = """
    MATCH (:Product {id:$pid})<-[:CONTAINS]-(o:Order)-[:CONTAINS]->(other:Product)
    WHERE other.id <> $pid
    WITH other, count(*) AS score
    RETURN other.id AS product_id, other.name AS name, score
    ORDER BY score DESC LIMIT $limit
    """
    with driver.session() as s:
        rows = s.run(query, {"pid": product_id, "limit": limit}).data()
    return {"product": product_id, "recommendations": rows}


@app.get("/recs/category/{customer_id}")
def recs_by_category(customer_id: str, limit: int = 10) -> Dict[str, Any]:
    """
    Category-based recommendations seeded from the user's interactions
    (VIEW/CLICK/ADD_TO_CART). Excludes items already purchased by the user.
    """
    query = """
    MATCH (c:Customer {id:$cid})-[:VIEW|CLICK|ADD_TO_CART]->(p:Product)-[:IN_CATEGORY]->(cat:Category)
    MATCH (rec:Product)-[:IN_CATEGORY]->(cat)
    WHERE NOT (c)-[:PLACED]->(:Order)-[:CONTAINS]->(rec)
    RETURN rec.id AS product_id, rec.name AS name, cat.name AS category, count(*) AS score
    ORDER BY score DESC LIMIT $limit
    """
    with driver.session() as s:
        rows = s.run(query, {"cid": customer_id, "limit": limit}).data()
    return {"customer": customer_id, "recommendations": rows}