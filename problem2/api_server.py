import json
import sys
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs, unquote

import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb", region_name="us-west-2")
TABLE_NAME = "arxiv-papers"


def query_recent_in_category(table_name, category, limit=20):
    response = dynamodb.Table(table_name).query(
        KeyConditionExpression=Key('PK').eq(f'CATEGORY#{category}'),
        ScanIndexForward=False,
        Limit=limit
    )
    return response['Items']


def query_papers_by_author(table_name, author_name):
    response = dynamodb.Table(table_name).query(
        IndexName='AuthorIndex',
        KeyConditionExpression=Key('GSI1PK').eq(f'AUTHOR#{author_name}')
    )
    return response['Items']


def get_paper_by_id(table_name, arxiv_id):
    response = dynamodb.Table(table_name).query(
        IndexName='PaperIdIndex',
        KeyConditionExpression=Key('GSI2PK').eq(f'PAPER#{arxiv_id}')
    )
    return response['Items'][0] if response['Items'] else None


def query_papers_in_date_range(table_name, category, start_date, end_date):

    response = dynamodb.Table(table_name).query(
        KeyConditionExpression=(
            Key('PK').eq(f'CATEGORY#{category}') &
            Key('SK').between(f'{start_date}#', f'{end_date}#zzzzzzz')
        )
    )
    return response['Items']


def query_papers_by_keyword(table_name, keyword, limit=20):

    response = dynamodb.Table(table_name).query(
        IndexName='KeywordIndex',
        KeyConditionExpression=Key('GSI3PK').eq(f'KEYWORD#{keyword.lower()}'),
        ScanIndexForward=False,
        Limit=limit
    )
    return response['Items']



class PaperRequestHandler(BaseHTTPRequestHandler):
    def _send_json(self, status_code, data):
        body = json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        sys.stdout.write("%s - - [%s] %s\n" %
                         (self.address_string(), self.log_date_time_string(), fmt % args))
        sys.stdout.flush()

    def do_GET(self):
        start = time.perf_counter()
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        try:
            if path == "/papers/recent":
                category = qs.get("category", [None])[0]
                limit = int(qs.get("limit", ["20"])[0])
                if not category:
                    self._send_json(400, {"error": "Missing category parameter"})
                    return
                items = query_recent_in_category(TABLE_NAME, category, limit)
                payload = {"category": category, "count": len(items), "papers": items}
                self._send_json(200, payload)
                return


            if path.startswith("/papers/author/"):
                author = unquote(path[len("/papers/author/"):])
                items = query_papers_by_author(TABLE_NAME, author)
                payload = {"author": author, "count": len(items), "papers": items}
                self._send_json(200, payload)
                return

    
            if (
                path.startswith("/papers/")
                and not path.startswith("/papers/author/")
                and not path.startswith("/papers/keyword/")
                and not path.startswith("/papers/search")
            ):
                arxiv_id = unquote(path[len("/papers/"):])
                item = get_paper_by_id(TABLE_NAME, arxiv_id)
                if item:
                    self._send_json(200, item)
                else:
                    self._send_json(404, {"error": "Paper not found", "arxiv_id": arxiv_id})
                return

         
            if path == "/papers/search":
                category = qs.get("category", [None])[0]
                start_date = qs.get("start", [None])[0]
                end_date = qs.get("end", [None])[0]
                if not (category and start_date and end_date):
                    self._send_json(400, {"error": "Missing category/start/end parameters"})
                    return
                items = query_papers_in_date_range(TABLE_NAME, category, start_date, end_date)
                payload = {
                    "category": category,
                    "start": start_date,
                    "end": end_date,
                    "count": len(items),
                    "papers": items,
                }
                self._send_json(200, payload)
                return

            if path.startswith("/papers/keyword/"):
                keyword = unquote(path[len("/papers/keyword/"):])
                limit = int(qs.get("limit", ["20"])[0])
                items = query_papers_by_keyword(TABLE_NAME, keyword, limit)
                payload = {"keyword": keyword, "count": len(items), "papers": items}
                self._send_json(200, payload)
                return

            self._send_json(404, {"error": "Unknown endpoint", "path": path})

        except Exception as e:
            self._send_json(500, {"error": "Internal server error", "message": str(e)})

        finally:
            elapsed_ms = int((time.perf_counter() - start) * 1000)
            self.log_message("Handled %s in %d ms", self.path, elapsed_ms)



def run_server(port=8080):
    server = HTTPServer(("", port), PaperRequestHandler)
    server.serve_forever()


if __name__ == "__main__":
    port = 8080
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            print("Invalid port; using default 8080.")
    run_server(port)

