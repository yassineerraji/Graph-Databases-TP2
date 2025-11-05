// ---------- SCHEMA (constraints & indexes) ----------

// Drop (optional during dev)
DROP CONSTRAINT customer_id IF EXISTS;
DROP CONSTRAINT product_id IF EXISTS;
DROP CONSTRAINT category_id IF EXISTS;
DROP CONSTRAINT order_id IF EXISTS;

// Create unique constraints
CREATE CONSTRAINT customer_id IF NOT EXISTS
FOR (c:Customer) REQUIRE c.id IS UNIQUE;

CREATE CONSTRAINT product_id IF NOT EXISTS
FOR (p:Product) REQUIRE p.id IS UNIQUE;

CREATE CONSTRAINT category_id IF NOT EXISTS
FOR (cat:Category) REQUIRE cat.id IS UNIQUE;

CREATE CONSTRAINT order_id IF NOT EXISTS
FOR (o:Order) REQUIRE o.id IS UNIQUE;

// Helpful lookup indexes
CREATE INDEX product_name IF NOT EXISTS FOR (p:Product) ON (p.name);
CREATE INDEX category_name IF NOT EXISTS FOR (c:Category) ON (c.name);

// ---------- QUICK STATS ----------
CALL db.labels() YIELD label RETURN label;
CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType;

// ---------- EXPLORATION QUERIES ----------
/*
Products in categories
*/
MATCH (p:Product)-[:IN_CATEGORY]->(cat:Category)
RETURN cat.name AS category, collect(p.name) AS products;

/*
Customer → Orders → Products path
*/
MATCH (c:Customer {id:'C1'})-[:PLACED]->(o:Order)-[:CONTAINS]->(p:Product)
RETURN o.id AS order, collect(p.name) AS items;

// ---------- RECOMMENDATION QUERIES ----------

/*
1) Co-occurrence (products often bought together)
   Given a product, find others co-purchased in same orders.
*/
:param productId => 'P1';
MATCH (:Product {id:$productId})<-[:CONTAINS]-(o:Order)-[:CONTAINS]->(other:Product)
WHERE other.id <> $productId
RETURN other.id AS product_id, other.name AS name, count(*) AS score
ORDER BY score DESC LIMIT 10;

/*
2) Customer-based co-occurrence:
   Given a customer, recommend items co-bought with her/his purchases.
*/
:param customerId => 'C1';
MATCH (c:Customer {id:$customerId})-[:PLACED]->(:Order)-[:CONTAINS]->(p:Product)
MATCH (o2:Order)-[:CONTAINS]->(p)
MATCH (o2)-[:CONTAINS]->(rec:Product)
WHERE NOT (c)-[:PLACED]->(:Order)-[:CONTAINS]->(rec)
RETURN rec.id AS product_id, rec.name AS name, count(*) AS score
ORDER BY score DESC LIMIT 10;

/*
3) Content-ish: recommend by category of items the customer interacted with
   (works even if they have only events, not orders)
*/
:param customerId => 'C1';
MATCH (c:Customer {id:$customerId})-[:VIEW|CLICK|ADD_TO_CART]->(p:Product)-[:IN_CATEGORY]->(cat:Category)
MATCH (rec:Product)-[:IN_CATEGORY]->(cat)
WHERE NOT (c)-[:PLACED]->(:Order)-[:CONTAINS]->(rec)
RETURN rec.id AS product_id, rec.name AS name, cat.name AS category, count(*) AS score
ORDER BY score DESC LIMIT 10;

/*
4) Jaccard similarity on views (item-item similarity)
   Using GDS to compute similarity across products viewed by customers.
*/
// Project a simple bipartite graph (Customer-Product via VIEW)
CALL gds.graph.drop('viewGraph', false);
CALL gds.graph.project(
  'viewGraph',
  ['Customer','Product'],
  {
    VIEW: {type: 'VIEW', orientation: 'UNDIRECTED'}
  }
);

// Compute Jaccard similarity between products
CALL gds.nodeSimilarity.stream('viewGraph', {nodeLabels:['Product']})
YIELD node1, node2, similarity
RETURN gds.util.asNode(node1).id AS p1,
       gds.util.asNode(node2).id AS p2,
       similarity
ORDER BY similarity DESC LIMIT 20;

/*
5) Personalized PageRank for a customer’s interests
   Seed with products they interacted with (VIEW/CLICK/ADD_TO_CART).
*/
CALL gds.graph.drop('productGraph', false);
CALL gds.graph.project(
  'productGraph',
  ['Product','Customer'],
  {
    VIEW: {type:'VIEW', orientation:'UNDIRECTED'},
    CLICK: {type:'CLICK', orientation:'UNDIRECTED'},
    ADD_TO_CART: {type:'ADD_TO_CART', orientation:'UNDIRECTED'},
    CONTAINS: {type:'CONTAINS', orientation:'UNDIRECTED'},
    PLACED: {type:'PLACED', orientation:'UNDIRECTED'},
    IN_CATEGORY: {type:'IN_CATEGORY', orientation:'UNDIRECTED'}
  }
);

// Build seed set for customer C1
MATCH (c:Customer {id:'C1'})-[:VIEW|CLICK|ADD_TO_CART]->(p:Product)
WITH collect(id(p)) AS seeds
CALL gds.pageRank.stream('productGraph', {sourceNodes: seeds})
YIELD nodeId, score
WITH gds.util.asNode(nodeId) AS n, score
WHERE n:Product
RETURN n.id AS product_id, n.name AS name, score
ORDER BY score DESC LIMIT 10;