# TD1 — Graph Recs (Neo4j + Postgres + FastAPI)

## Run
```bash
docker compose up -d
curl http://localhost:8000/health   # expects {"ok":true}
```

ETL runs automatically on app start (RUN_ETL=1). To run manually:
```bash
docker compose exec app python etl.py   # prints: ETL done.
```

## API
- GET `/health`
- GET `/recs/{customer_id}?limit=5`
- GET `/recs/product/{product_id}?limit=10`
- GET `/recs/category/{customer_id}?limit=10`

## Neo4j Browser
- Open `http://localhost:7474`
- Login: user `neo4j`, password in `Neo4j-info.txt`

Example:
```cypher
MATCH (c:Customer) RETURN count(c) AS customers;
```

## Outputs (screenshots & answers)
- See `Submission/` folder:
  - `Submission/Constraints.png`
  - `Submission/answers.txt`

## Reset
```bash
docker compose down -v && docker compose up -d
```# Graph-Databases-TD1
