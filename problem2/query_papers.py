import argparse
import json
import time

import boto3
from boto3.dynamodb.conditions import Key


dynamodb = boto3.resource("dynamodb", region_name="us-west-2")




def query_recent_in_category(table_name, category, limit=20):
    """
    Query 1: Browse recent papers in category.
    Uses: Main table partition key query with sort key descending.
    """
    response = dynamodb.Table(table_name).query(
        KeyConditionExpression=Key('PK').eq(f'CATEGORY#{category}'),
        ScanIndexForward=False,
        Limit=limit
    )
    return response['Items']

def query_papers_by_author(table_name, author_name):
    """
    Query 2: Find all papers by author.
    Uses: GSI1 (AuthorIndex) partition key query.
    """
    response = dynamodb.Table(table_name).query(
        IndexName='AuthorIndex',
        KeyConditionExpression=Key('GSI1PK').eq(f'AUTHOR#{author_name}')
    )
    return response['Items']



def get_paper_by_id(table_name, arxiv_id):
    """
    Query 3: Get specific paper by ID.
    Uses: GSI2 (PaperIdIndex) for direct lookup.
    """
    response = dynamodb.Table(table_name).query(
        IndexName='PaperIdIndex',
        KeyConditionExpression=Key('GSI2PK').eq(f'PAPER#{arxiv_id}')
    )
    return response['Items'][0] if response['Items'] else None



def query_papers_in_date_range(table_name, category, start_date, end_date):
    """
    Query 4: Papers in category within date range.
    Uses: Main table with composite sort key range query.
    """
    response = dynamodb.Table(table_name).query(
        KeyConditionExpression=(
            Key('PK').eq(f'CATEGORY#{category}') &
            Key('SK').between(f'{start_date}#', f'{end_date}#zzzzzzz')
        )
    )
    return response['Items']


def query_papers_by_keyword(table_name, keyword, limit=20):
    """
    Query 5: Papers containing keyword.
    Uses: GSI3 (KeywordIndex) partition key query.
    """
    response = dynamodb.Table(table_name).query(
        IndexName='KeywordIndex',
        KeyConditionExpression=Key('GSI3PK').eq(f'KEYWORD#{keyword.lower()}'),
        ScanIndexForward=False,
        Limit=limit
    )
    return response['Items']


def main():
    parser = argparse.ArgumentParser(description="Query ArXiv DynamoDB table.")
    subparsers = parser.add_subparsers(dest="command", required=True)


    p1 = subparsers.add_parser("recent", help="Recent papers in a category")
    p1.add_argument("category")
    p1.add_argument("--limit", type=int, default=20)
    p1.add_argument("--table", default="arxiv-papers")


    p2 = subparsers.add_parser("author", help="Papers by author")
    p2.add_argument("author_name")
    p2.add_argument("--table", default="arxiv-papers")


    p3 = subparsers.add_parser("get", help="Get paper by ID")
    p3.add_argument("arxiv_id")
    p3.add_argument("--table", default="arxiv-papers")

    p4 = subparsers.add_parser("daterange", help="Papers in category within date range")
    p4.add_argument("category")
    p4.add_argument("start_date")
    p4.add_argument("end_date")
    p4.add_argument("--table", default="arxiv-papers")


    p5 = subparsers.add_parser("keyword", help="Papers by keyword")
    p5.add_argument("keyword")
    p5.add_argument("--limit", type=int, default=20)
    p5.add_argument("--table", default="arxiv-papers")

    args = parser.parse_args()
    start = time.perf_counter()

    if args.command == "recent":
        items = query_recent_in_category(args.table, args.category, args.limit)
        result = {
            "query_type": "recent_in_category",
            "parameters": {"category": args.category, "limit": args.limit},
            "results": items,
            "count": len(items),
        }

    elif args.command == "author":
        items = query_papers_by_author(args.table, args.author_name)
        result = {
            "query_type": "papers_by_author",
            "parameters": {"author_name": args.author_name},
            "results": items,
            "count": len(items),
        }

    elif args.command == "get":
        item = get_paper_by_id(args.table, args.arxiv_id)
        result = {
            "query_type": "paper_by_id",
            "parameters": {"arxiv_id": args.arxiv_id},
            "result": item,
            "found": bool(item),
        }

    elif args.command == "daterange":
        items = query_papers_in_date_range(args.table, args.category, args.start_date, args.end_date)
        result = {
            "query_type": "papers_in_date_range",
            "parameters": {
                "category": args.category,
                "start_date": args.start_date,
                "end_date": args.end_date,
            },
            "results": items,
            "count": len(items),
        }

    elif args.command == "keyword":
        items = query_papers_by_keyword(args.table, args.keyword, args.limit)
        result = {
            "query_type": "papers_by_keyword",
            "parameters": {"keyword": args.keyword, "limit": args.limit},
            "results": items,
            "count": len(items),
        }

    else:
        raise ValueError("Unknown command")

    end = time.perf_counter()
    result["execution_time_ms"] = int((end - start) * 1000)

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
