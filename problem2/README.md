1. Schema Design Decisions

I used a single-table design with a composite primary key (PK, SK) to support multiple access patterns efficiently.
The partition key always encodes the entity type (e.g., CATEGORY#, AUTHOR#, KEYWORD#, PAPER#),
and the sort key stores the date and paper ID (e.g., 2023-10-01#2301.12345), allowing chronological queries.

I created three GSIs:

AuthorIndex → query all papers by a specific author

PaperIdIndex → direct lookup by paper ID

KeywordIndex → search all papers containing a given keyword

These GSIs cover all five required query types (category, author, paper ID, date range, keyword).

The main denormalization trade-off is storage duplication:
Each paper is stored multiple times under different keys (category, author, keyword, ID).
This improves query performance but increases storage and update costs.

2. Denormalization Analysis

After loading the dataset, each paper produced around 15 DynamoDB items on average.
The storage multiplication factor is about 15×, meaning one logical record becomes 15 physical items.

The most duplicated access pattern is the keyword-based search,
because each paper typically includes around 10 keywords,
so it generates the largest share of duplicated entries.

3. Query Limitations

The schema cannot efficiently support global aggregation or multi-part queries such as:

Counting total papers by each author

Finding the most cited or most popular papers

Global keyword frequency statistics

These are difficult in DynamoDB because it does not support joins or aggregation.
Such queries would require a full table scan or external analytics pipeline (e.g., Spark or Athena).

4. When to Use DynamoDB

DynamoDB is ideal when:

Access patterns are known and fixed in advance

You need high scalability, automatic partitioning, and sub-100ms latency

You can tolerate denormalization and eventual consistency

PostgreSQL is preferred when:

You need flexible queries, joins, transactions, and strong consistency

You perform complex analytics or ad-hoc reporting

Trade-off summary: DynamoDB favors speed and scalability;
PostgreSQL favors flexibility and relational integrity.

5. EC2 Deployment

EC2 Public IP: 3.80.222.85
IAM Role ARN: arn:aws:iam::537736509366:role/EE547DynamoDBRole

Challenges:
- Ensuring port 8080 was open in the EC2 security group.
- Waiting for DynamoDB GSIs to reach ACTIVE status before querying.
- Keeping the API server running persistently using nohup.