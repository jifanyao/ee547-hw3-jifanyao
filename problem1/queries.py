import psycopg2
import argparse
import json
import sys

def run_query(cur, qname):

    if qname == "Q1":
        description = "List all stops on Route 20 in order"
        sql = """
        SELECT s.stop_name, ls.sequence, ls.time_offset
        FROM line_stops ls
        JOIN lines l ON ls.line_id = l.line_id
        JOIN stops s ON ls.stop_id = s.stop_id
        WHERE l.line_name = 'Route 20'
        ORDER BY ls.sequence;
        """

    elif qname == "Q2":
        description = "Trips during morning rush (7-9 AM)"
        sql = """
        SELECT t.trip_id, l.line_name, t.scheduled_departure
        FROM trips t
        JOIN lines l ON t.line_id = l.line_id
        WHERE EXTRACT(HOUR FROM t.scheduled_departure) BETWEEN 7 AND 9
        ORDER BY t.scheduled_departure, t.trip_id;
        """

    elif qname == "Q3":
        description = "Transfer stops (stops on 2+ routes)"
        sql = """
        SELECT s.stop_name, COUNT(DISTINCT ls.line_id) AS line_count
        FROM line_stops ls
        JOIN stops s ON ls.stop_id = s.stop_id
        GROUP BY s.stop_name
        HAVING COUNT(DISTINCT ls.line_id) >= 2
        ORDER BY line_count DESC, s.stop_name;
        """

    elif qname == "Q4":
        description = "Complete route for trip T0001"
        sql = """
        SELECT s.stop_name,
               ls.sequence,
               se.scheduled,
               se.actual
        FROM trips t
        JOIN lines l ON t.line_id = l.line_id
        JOIN line_stops ls ON ls.line_id = l.line_id
        JOIN stops s ON s.stop_id = ls.stop_id
        LEFT JOIN stop_events se
               ON se.trip_id = t.trip_id
              AND se.stop_id = s.stop_id
        WHERE t.trip_id = 'T0001'
        ORDER BY ls.sequence;
        """

    elif qname == "Q5":
        description = "Routes serving both Wilshire / Veteran and Le Conte / Broxton"
        sql = """
        SELECT l.line_name
        FROM lines l
        JOIN line_stops ls ON l.line_id = ls.line_id
        JOIN stops s ON s.stop_id = ls.stop_id
        WHERE s.stop_name IN ('Wilshire / Veteran', 'Le Conte / Broxton')
        GROUP BY l.line_name
        HAVING COUNT(DISTINCT s.stop_name) = 2
        ORDER BY l.line_name;
        """

    elif qname == "Q6":
        description = "Average ridership by line"
        sql = """
        SELECT l.line_name,
               AVG(se.passengers_on + se.passengers_off) AS avg_passengers
        FROM lines l
        JOIN trips t ON t.line_id = l.line_id
        JOIN stop_events se ON se.trip_id = t.trip_id
        GROUP BY l.line_name
        ORDER BY avg_passengers DESC;
        """

    elif qname == "Q7":
        description = "Top 10 busiest stops"
        sql = """
        SELECT s.stop_name,
               SUM(se.passengers_on + se.passengers_off) AS total_activity
        FROM stops s
        JOIN stop_events se ON se.stop_id = s.stop_id
        GROUP BY s.stop_name
        ORDER BY total_activity DESC, s.stop_name
        LIMIT 10;
        """

    elif qname == "Q8":
        description = "Count delays by line (>2 min late)"
        sql = """
        SELECT l.line_name,
               COUNT(*) AS delay_count
        FROM lines l
        JOIN trips t ON t.line_id = l.line_id
        JOIN stop_events se ON se.trip_id = t.trip_id
        WHERE se.actual > se.scheduled + INTERVAL '2 minutes'
        GROUP BY l.line_name
        ORDER BY delay_count DESC, l.line_name;
        """

    elif qname == "Q9":
        description = "Trips with 3+ delayed stops"
        sql = """
        SELECT se.trip_id,
               COUNT(*) AS delayed_stop_count
        FROM stop_events se
        WHERE se.actual > se.scheduled + INTERVAL '2 minutes'
        GROUP BY se.trip_id
        HAVING COUNT(*) >= 3
        ORDER BY delayed_stop_count DESC, se.trip_id;
        """

    elif qname == "Q10":
        description = "Stops with above-average ridership"
        sql = """
        SELECT s.stop_name,
               SUM(se.passengers_on) AS total_boardings
        FROM stops s
        JOIN stop_events se ON se.stop_id = s.stop_id
        GROUP BY s.stop_name
        HAVING SUM(se.passengers_on) >
               (
                 SELECT AVG(totals.total_boardings)
                 FROM (
                   SELECT SUM(se2.passengers_on) AS total_boardings
                   FROM stop_events se2
                   GROUP BY se2.stop_id
                 ) AS totals
               )
        ORDER BY total_boardings DESC, s.stop_name;
        """

    else:
        raise ValueError(f"Unknown query name: {qname}")

    cur.execute(sql)
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    results = [dict(zip(columns, row)) for row in rows]
    return description, results

def main():
    parser = argparse.ArgumentParser(description="Execute metro transit SQL queries")
    parser.add_argument("--query", help="Query name (Q1..Q10)")
    parser.add_argument("--all", action="store_true", help="Run all queries Q1..Q10")
    parser.add_argument("--host", default="db", help="Database host (default: db for Docker)")
    parser.add_argument("--port", type=int, default=5432, help="Database port")
    parser.add_argument("--dbname", default="transit", help="Database name")
    parser.add_argument("--user", default="transit", help="Database user (default: transit)")
    parser.add_argument("--password", default="transit123", help="Database password (default: transit123)")
    parser.add_argument("--format", default="json", help="Output format (only 'json' is supported)")
    args = parser.parse_args()

    if not args.all and not args.query:
        print("Error: must provide --query Q1..Q10 or --all", file=sys.stderr)
        sys.exit(1)

    if args.format.lower() != "json":
        print("Error: only JSON output is supported (--format json)", file=sys.stderr)
        sys.exit(1)

    try:
        conn = psycopg2.connect(
            host=args.host,
            port=args.port,
            dbname=args.dbname,
            user=args.user,
            password=args.password,
        )
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}", file=sys.stderr)
        sys.exit(1)

    cur = conn.cursor()

    if args.all:
        query_names = [f"Q{i}" for i in range(1, 11)]
    else:
        query_names = [args.query]

    first = True
    for qname in query_names:
        description, results = run_query(cur, qname)
        output = {
            "query": qname,
            "description": description,
            "results": results,
            "count": len(results),
        }
        if not first:
            print()
        print(json.dumps(output, indent=2, default=str))
        first = False

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()