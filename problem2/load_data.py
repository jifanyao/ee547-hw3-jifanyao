import argparse
import json
import re
import time
from collections import Counter
from datetime import datetime
import boto3

STOPWORDS = {
    'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
    'of', 'with', 'by', 'from', 'up', 'about', 'into', 'through', 'during',
    'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
    'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might',
    'can', 'this', 'that', 'these', 'those', 'we', 'our', 'use', 'using',
    'based', 'approach', 'method', 'paper', 'propose', 'proposed', 'show'
}

def parse_args():
    parser = argparse.ArgumentParser(
        description="Load ArXiv papers JSON into DynamoDB with denormalized schema."
    )
    parser.add_argument("papers_json_path", help="Path to papers.json from HW1 Problem 2")
    parser.add_argument("table_name", help="DynamoDB table name to create/use")
    parser.add_argument(
        "--region",
        dest="region",
        default="us-west-2",
        help="AWS region name (default: us-west-2)",
    )
    return parser.parse_args()



def get_dynamodb_resource(region: str):
    print(f"Using DynamoDB region: {region}")
    return boto3.resource("dynamodb", region_name=region)

def ensure_table_exists(dynamodb, table_name: str):
    client = dynamodb.meta.client
    table = dynamodb.Table(table_name)
    try:
        table.load()
        print(f"Using existing DynamoDB table: {table_name}")
        return table
    except client.exceptions.ResourceNotFoundException:
        pass

    print(f"Creating DynamoDB table: {table_name}")
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
            {"AttributeName": "GSI1PK", "AttributeType": "S"},
            {"AttributeName": "GSI1SK", "AttributeType": "S"},
            {"AttributeName": "GSI2PK", "AttributeType": "S"},
            {"AttributeName": "GSI2SK", "AttributeType": "S"},
            {"AttributeName": "GSI3PK", "AttributeType": "S"},
            {"AttributeName": "GSI3SK", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "AuthorIndex",
                "KeySchema": [
                    {"AttributeName": "GSI1PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI1SK", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 5,
                    "WriteCapacityUnits": 5,
                },
            },
            {
                "IndexName": "PaperIdIndex",
                "KeySchema": [
                    {"AttributeName": "GSI2PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI2SK", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 5,
                    "WriteCapacityUnits": 5,
                },
            },
            {
                "IndexName": "KeywordIndex",
                "KeySchema": [
                    {"AttributeName": "GSI3PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI3SK", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 5,
                    "WriteCapacityUnits": 5,
                },
            },
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )

    table.wait_until_exists()

    print("Waiting for all Global Secondary Indexes (GSI) to become ACTIVE...")
    while True:
        desc = client.describe_table(TableName=table_name)
        gsi_statuses = [gsi["IndexStatus"] for gsi in desc.get("Table", {}).get("GlobalSecondaryIndexes", [])]
        if all(s == "ACTIVE" for s in gsi_statuses):
            print("✅ All GSI indexes are ACTIVE.")
            break
        else:
            print("⏳ GSI not ready yet:", gsi_statuses)
            time.sleep(5)

    return table


def normalize_published(published_raw: str | None):
    if not published_raw:
        return "1970-01-01", "1970-01-01T00:00:00Z"
    s = str(published_raw)
    try:
        if "T" in s:
            s2 = s.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s2)
        else:
            dt = datetime.fromisoformat(s.strip() + "T00:00:00")
        date_str = dt.date().isoformat()
        iso_str = dt.isoformat().replace("+00:00", "Z")
        return date_str, iso_str
    except Exception:
        return s[:10], s


def extract_keywords(abstract: str, max_keywords: int = 10):
    if not abstract:
        return []
    tokens = re.findall(r"[a-zA-Z]+", abstract.lower())
    tokens = [t for t in tokens if t not in STOPWORDS]
    if not tokens:
        return []
    counter = Counter(tokens)
    return [w for (w, _) in counter.most_common(max_keywords)]


def load_papers_from_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "papers" in data:
        return data["papers"]
    raise ValueError("Unsupported papers.json format: expected list or {'papers': [...]}.")

def main():
    args = parse_args()
    dynamodb = get_dynamodb_resource(args.region)
    table = ensure_table_exists(dynamodb, args.table_name)

    print(f"Loading papers from {args.papers_json_path}...")
    papers = load_papers_from_json(args.papers_json_path)

    num_papers = 0
    total_items = 0
    category_items = author_items = keyword_items = id_items = 0

    with table.batch_writer(overwrite_by_pkeys=("PK", "SK")) as batch:
        for paper in papers:
            arxiv_id = str(paper.get("arxiv_id") or paper.get("id") or "").strip()
            if not arxiv_id:
                continue

            title = paper.get("title", "").strip()
            abstract = paper.get("abstract", "").strip()
            authors = paper.get("authors") or []
            categories = paper.get("categories") or []
            if not isinstance(authors, list):
                authors = [str(authors)]
            if not isinstance(categories, list):
                categories = [str(categories)]

            date_str, published_iso = normalize_published(paper.get("published"))
            keywords = extract_keywords(abstract, max_keywords=10)

            base_attrs = {
                "arxiv_id": arxiv_id,
                "title": title,
                "authors": authors,
                "abstract": abstract,
                "categories": categories,
                "keywords": keywords,
                "published": published_iso,
            }

            num_papers += 1

            for cat in categories:
                item = {
                    "PK": f"CATEGORY#{cat}",
                    "SK": f"{date_str}#{arxiv_id}",
                    "item_type": "CATEGORY",
                }
                item.update(base_attrs)
                batch.put_item(Item=item)
                category_items += 1
                total_items += 1

            for author in authors:
                author_str = str(author).strip()
                if not author_str:
                    continue
                item = {
                    "PK": f"AUTHOR#{author_str}",
                    "SK": f"{date_str}#{arxiv_id}",
                    "GSI1PK": f"AUTHOR#{author_str}",
                    "GSI1SK": f"{date_str}#{arxiv_id}",
                    "item_type": "AUTHOR",
                }
                item.update(base_attrs)
                batch.put_item(Item=item)
                author_items += 1
                total_items += 1


            for kw in keywords:
                kw_str = kw.lower().strip()
                if not kw_str:
                    continue
                item = {
                    "PK": f"KEYWORD#{kw_str}",
                    "SK": f"{date_str}#{arxiv_id}",
                    "GSI3PK": f"KEYWORD#{kw_str}",
                    "GSI3SK": f"{date_str}#{arxiv_id}",
                    "item_type": "KEYWORD",
                }
                item.update(base_attrs)
                batch.put_item(Item=item)
                keyword_items += 1
                total_items += 1

            id_item = {
                "PK": f"PAPER#{arxiv_id}",
                "SK": "PAPER",
                "GSI2PK": f"PAPER#{arxiv_id}",
                "GSI2SK": "PAPER",
                "item_type": "PAPER",
            }
            id_item.update(base_attrs)
            batch.put_item(Item=id_item)
            id_items += 1
            total_items += 1

    print(f"Loaded {num_papers} papers")
    print(f"Created {total_items} DynamoDB items (denormalized)")
    factor = total_items / float(num_papers) if num_papers else 0.0
    print(f"Denormalization factor: {factor:.1f}x\n")
    print("Storage breakdown:")
    if num_papers:
        print(f"  - Category items: {category_items} ({category_items / num_papers:.1f} per paper avg)")
        print(f"  - Author items:   {author_items} ({author_items / num_papers:.1f} per paper avg)")
        print(f"  - Keyword items:  {keyword_items} ({keyword_items / num_papers:.1f} per paper avg)")
        print(f"  - Paper ID items: {id_items} ({id_items / num_papers:.1f} per paper avg)")
    else:
        print(f"  - Category items: {category_items}")
        print(f"  - Author items:   {author_items}")
        print(f"  - Keyword items:  {keyword_items}")
        print(f"  - Paper ID items: {id_items}")


if __name__ == "__main__":
    main()
