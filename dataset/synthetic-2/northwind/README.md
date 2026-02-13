# Northwind Dataset

**Source:** `synthetic_gpt4o_demodbs` (Neo4j text2cypher)

**Total valid queries: 807**

Products, orders, suppliers, customers, categories.

## Current Status
- `queries.csv`: 783 converted queries
- 24 failed queries

Total: 783 + 24 = 807 / 807 ✓

## Failed Queries (24 total)

All 24 failures involve the `freight` attribute, which is `value string` in the TypeQL schema. The English questions treat freight as a numeric value (comparing, sorting, summing, averaging), but TypeQL cannot cast strings to numbers at runtime. Lexicographic string comparison would give incorrect results (e.g., "9" > "100"). This will be unblocked when **type casting functions** (e.g., `to_double()`) are implemented in TypeQL.

### Query 66
**Reason:** Requires numeric comparison on string `freight` (`toFloat(freight) > 100`).
```cypher
MATCH (o:Order)
WHERE toFloat(o.freight) > 100
RETURN o.shipName, toFloat(o.freight)
ORDER BY toFloat(o.freight) DESC
LIMIT 3
```

### Query 78
**Reason:** Requires `avg(toFloat(freight))` — numeric aggregation on string attribute.
```cypher
MATCH (s:Supplier)-[:SUPPLIES]->(p:Product)<-[:ORDERS]-(o:Order)
WITH s, o.freight AS freight
WHERE freight IS NOT NULL
WITH s, avg(toFloat(freight)) AS avgFreight
ORDER BY avgFreight DESC
LIMIT 3
RETURN s.companyName AS supplierName, avgFreight
```

### Query 98
**Reason:** Requires numeric comparison on string `freight` (`toFloat(freight) > 1000`).
```cypher
MATCH (c:Customer)-[:PURCHASED]->(o:Order)
WHERE toFloat(o.freight) > 1000
RETURN c.companyName, c.contactName, c.contactTitle, c.phone, c.address, c.city, c.country
ORDER BY o.orderDate
LIMIT 3
```

### Query 145
**Reason:** Requires numeric comparison on string `freight` (`toFloat(freight) > 100`).
```cypher
MATCH (o:Order)-[:ORDERS]->(p:Product)
WHERE o.shipCity = 'Berlin' AND toFloat(o.freight) > 100
RETURN o.orderID, o.shipName, o.shipAddress, o.shipCity, o.shipPostalCode, o.shipCountry, o.freight
ORDER BY o.orderDate
LIMIT 3
```

### Query 148
**Reason:** Requires `SUM(toFloat(freight))` — numeric aggregation on string attribute.
```cypher
MATCH (c:Customer)-[:PURCHASED]->(o:Order)
WITH c, SUM(toFloat(o.freight)) AS totalFreight
ORDER BY totalFreight DESC
LIMIT 5
RETURN c.companyName AS customerName, totalFreight
```

### Query 161
**Reason:** Requires numeric comparison on string `freight` (`toFloat(freight) > 200`).
```cypher
MATCH (o:Order)
WHERE o.requiredDate < '1997-06-01' AND toFloat(o.freight) > 200
RETURN o.orderID, o.requiredDate, o.freight
ORDER BY o.requiredDate
LIMIT 3
```

### Query 167
**Reason:** Requires numeric comparison on string `freight` (`toFloat(freight) < 50`).
```cypher
MATCH (o:Order)
WHERE o.shipCity = 'Reims' AND toFloat(o.freight) < 50
RETURN o.orderID, o.shipName, o.requiredDate, o.shipCity, o.shipPostalCode, o.shippedDate, o.freight, o.orderDate, o.shipAddress, o.customerID, o.shipCountry, o.shipVia, o.shipRegion
```

### Query 176
**Reason:** Requires numeric comparison on string `freight` (`toFloat(freight) > 50`).
```cypher
MATCH (o:Order)-[:ORDERS]->(p:Product)
WHERE o.shipCountry = 'France' AND toFloat(o.freight) > 50
RETURN o.orderID, o.shipName, o.shipCity, o.shipPostalCode, o.shipAddress, o.shipCountry, o.freight
```

### Query 261
**Reason:** Requires `SUM(toFloat(freight))` — numeric aggregation on string attribute. Same as Query 148.
```cypher
MATCH (c:Customer)-[:PURCHASED]->(o:Order)
WITH c, SUM(toFloat(o.freight)) AS totalFreight
ORDER BY totalFreight DESC
LIMIT 5
RETURN c.companyName AS customerName, totalFreight
```

### Query 269
**Reason:** Requires numeric sort on string `freight` (`ORDER BY toFloat(freight)`).
```cypher
MATCH (o:Order)
RETURN o.orderID, o.freight
ORDER BY toFloat(o.freight) DESC
LIMIT 3
```

### Query 308
**Reason:** Requires numeric comparison on string `freight` (`toFloat(freight) > 100`).
```cypher
MATCH (c:Customer)-[:PURCHASED]->(o:Order)
WHERE toFloat(o.freight) > 100
RETURN c.companyName AS customerName, c.contactName AS contactName, c.contactTitle AS contactTitle, o.orderID AS orderID, o.freight AS freightCost
```

### Query 322
**Reason:** Requires numeric comparison on string `freight` (`toFloat(freight) < 10`).
```cypher
MATCH (o:Order)
WHERE toFloat(o.freight) < 10
RETURN o.orderID, o.shipName, o.requiredDate, o.shipCity, o.shipPostalCode, o.shippedDate, o.freight, o.orderDate, o.shipAddress, o.customerID, o.shipCountry, o.shipVia, o.shipRegion
```

### Query 375
**Reason:** Requires numeric comparison on string `freight` (`toFloat(freight) > 100`). Same as Query 66.
```cypher
MATCH (o:Order)
WHERE toFloat(o.freight) > 100
RETURN o.orderID, o.freight
ORDER BY toFloat(o.freight) DESC
LIMIT 3
```

### Query 402
**Reason:** Requires numeric comparison on string `freight` (`toFloat(freight) > 100`).
```cypher
MATCH (o:Order)
WHERE o.orderDate < '1997-01-01' AND toFloat(o.freight) > 100
RETURN o.orderID, o.orderDate, o.freight
```

### Query 470
**Reason:** Requires numeric comparison on string `freight` (`toFloat(freight) > 10`).
```cypher
MATCH (c:Customer {companyName: 'Alfreds Futterkiste'})-[:PURCHASED]->(o:Order)
WHERE toFloat(o.freight) > 10
RETURN o.orderID, o.orderDate, o.freight
ORDER BY o.orderDate
LIMIT 3
```

### Query 515
**Reason:** Requires `SUM(toFloat(freight))` — numeric aggregation on string attribute. Same as Query 148.
```cypher
MATCH (c:Customer)-[:PURCHASED]->(o:Order)
WITH c, SUM(toFloat(o.freight)) AS totalFreight
ORDER BY totalFreight DESC
LIMIT 5
RETURN c.companyName AS customerName, totalFreight
```

### Query 520
**Reason:** Requires numeric comparison on string `freight` (`toFloat(freight) < 15`).
```cypher
MATCH (o:Order)
WHERE toFloat(o.freight) < 15
RETURN o.orderID, o.freight, o.orderDate
ORDER BY o.orderDate
LIMIT 3
```

### Query 522
**Reason:** Requires `AVG(toFloat(freight))` — numeric aggregation on string attribute. Same as Query 78.
```cypher
MATCH (s:Supplier)-[:SUPPLIES]->(p:Product)<-[:ORDERS]-(o:Order)
WITH s, o.freight AS freight
WHERE freight IS NOT NULL
RETURN s.companyName AS supplier, AVG(toFloat(freight)) AS avgFreightCost
ORDER BY avgFreightCost DESC
LIMIT 5
```

### Query 634
**Reason:** Requires numeric comparison on string `freight` (`toFloat(freight) > 250`).
```cypher
MATCH (o:Order)-[:ORDERS]->(p:Product)
WHERE toFloat(o.freight) > 250
RETURN p.productName AS productName, o.freight AS freight
LIMIT 5
```

### Query 637
**Reason:** Requires numeric comparison on string `freight` (`toFloat(freight) > 25`).
```cypher
MATCH (o:Order)
WHERE o.orderDate STARTS WITH '1997' AND toFloat(o.freight) > 25
RETURN o.orderID, o.orderDate, o.freight
ORDER BY o.orderDate
LIMIT 3
```

### Query 672
**Reason:** Requires numeric comparison on string `freight` (`toFloat(freight) < 5`).
```cypher
MATCH (o:Order)
WHERE o.requiredDate STARTS WITH '1998' AND toFloat(o.freight) < 5
RETURN o.orderID, o.requiredDate, o.freight
```

### Query 718
**Reason:** Requires numeric comparison on string `freight` (`toFloat(freight) > 50`).
```cypher
MATCH (o:Order)
WHERE toFloat(o.freight) > 50
RETURN o.orderID, o.shipName, o.requiredDate, o.shipCity, o.employeeID, o.shipPostalCode, o.shippedDate, o.freight, o.orderDate, o.shipAddress, o.customerID, o.shipCountry, o.shipVia, o.shipRegion
```

### Query 767
**Reason:** Requires `SUM(toFloat(freight))` — numeric aggregation on string attribute. Same as Query 148.
```cypher
MATCH (c:Customer)-[:PURCHASED]->(o:Order)
WITH c, SUM(toFloat(o.freight)) AS totalFreight
ORDER BY totalFreight DESC
LIMIT 5
RETURN c.companyName AS customerName, totalFreight
```

### Query 789
**Reason:** Requires numeric sort on string `freight` (`ORDER BY toFloat(freight)`). Same as Query 269.
```cypher
MATCH (o:Order)
RETURN o.orderID, o.freight
ORDER BY toFloat(o.freight) DESC
LIMIT 5
```

## Resolved Queries

### Query 262 (resolved)
Previously failed due to `collect()` + `UNWIND` for product co-occurrence. Converted using a self-join on the `orders` relation to find product pairs appearing in the same order, with `$id1 < $id2` to deduplicate pairs.

### Query 399 (resolved)
Previously failed due to `COLLECT()` + `REDUCE` + `RANGE` + `SIZE()` for computing cumulative price variation. Converted using `max_order_price()` and `min_order_price()` custom functions on the `order_unit_price` attribute of the `orders` relation, computing variation as `$max - $min`.

### Query 689 (resolved)
Previously failed due to schema mismatch (no `Employee` entity or `PROCESSED` relation). Converted to return distinct `employee_id` values from orders placed by the specified customer, which is the closest available data in the schema.
