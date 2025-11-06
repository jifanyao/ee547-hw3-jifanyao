# ee547-hw3-jifanyao
full name: jifanyao
Usc email: jifanyao@usc.edu
Instructions to Run

Problem 1

docker-compose up  
python load_data.py  
python test_queries.py  


Problem 2

python load_data.py data/papers.json arxiv-papers --region us-west-2  
python query_papers.py recent cs.LG --limit 5 --table arxiv-papers  
python api_server.py 8080  
curl "http://<EC2-Public-IP>:8080/papers/recent?category=cs.LG&limit=5"

AWS Region

EC2 instance deployed in us-east-1 (N. Virginia)
DynamoDB table deployed in us-west-2 (Oregon)

Design Decisions and Trade-offs

Used a single-table design (PK, SK) structure.

PK encodes the entity type (e.g., CATEGORY#cs.LG).

SK stores date and paper ID to support chronological queries.
Created three GSIs — AuthorIndex, PaperIdIndex, and KeywordIndex — to cover all five query types.
Each paper generates about 15 items due to denormalization, trading extra storage for much faster query performance.
