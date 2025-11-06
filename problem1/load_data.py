import psycopg2
import csv
import argparse
import os
import sys

def execute_sql_file(cursor, data_path):
    with open(data_path, 'r', encoding="utf-8") as f:
        sql = f.read()
    cursor.execute(sql)

def build_line_map(cur):
    cur.execute("SELECT line_id, line_name FROM lines;")
    return {name: line_id for (line_id, name) in cur.fetchall()}

def build_stop_map(cur):
    cur.execute("SELECT stop_id, stop_name FROM stops;")
    return {name: stop_id for (stop_id, name) in cur.fetchall()}

def load_lines(cur, datadir):
    path = os.path.join(datadir, 'lines.csv')
    count = 0
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cur.execute(
                """
                INSERT INTO lines (line_name, vehicle_type)
                VALUES (%s, %s)
                ON CONFLICT (line_name) DO NOTHING;
                """,
                (row["line_name"], row["vehicle_type"])
            )
            count += 1
    return count

def load_stops(cur, datadir):
    path = os.path.join(datadir, "stops.csv")
    count = 0
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cur.execute(
                """
                INSERT INTO stops (stop_name, latitude, longitude)
                VALUES (%s, %s, %s)
                ON CONFLICT (stop_name) DO NOTHING;
                """,
                (row["stop_name"], row["latitude"], row["longitude"])
            )
            count += 1
    return count

def load_line_stops(cur, datadir, line_map, stop_map):
    path = os.path.join(datadir, "line_stops.csv")
    count = 0
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            line_name = row["line_name"]
            stop_name = row["stop_name"]
            line_id = line_map[line_name]
            stop_id = stop_map[stop_name]
            sequence = int(row["sequence"])
            time_offset = int(row["time_offset"])
            cur.execute(
                """
                INSERT INTO line_stops (line_id, stop_id, sequence, time_offset)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (line_id, stop_id) DO NOTHING;
                """,
                (line_id, stop_id, sequence, time_offset)
            )
            count += 1
    return count

def load_trips(cur, datadir, line_map):
    path = os.path.join(datadir, "trips.csv")
    count = 0
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            line_name = row["line_name"]
            line_id = line_map[line_name]
            cur.execute(
                """
                INSERT INTO trips (trip_id, line_id, scheduled_departure, vehicle_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (trip_id) DO NOTHING;
                """,
                (row["trip_id"], line_id, row["scheduled_departure"], row["vehicle_id"])
            )
            count += 1
    return count

def load_stop_events(cur, datadir, stop_map):
    path = os.path.join(datadir, "stop_events.csv")
    count = 0
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            stop_name = row["stop_name"]
            stop_id = stop_map[stop_name]
            cur.execute(
                """
                INSERT INTO stop_events (
                    trip_id, stop_id, scheduled, actual,
                    passengers_on, passengers_off
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
                """,
                (
                    row["trip_id"],
                    stop_id,
                    row["scheduled"],
                    row["actual"],
                    row["passengers_on"],
                    row["passengers_off"],
                )
            )
            count += 1
    return count

def main():
    parser = argparse.ArgumentParser(description="Load metro transit CSV data into PostgreSQL")
    parser.add_argument("--host", default="localhost", help="Database host")
    parser.add_argument("--port", type=int, default=5432, help="Database port")
    parser.add_argument("--dbname", default="transit", help="Database name")
    parser.add_argument("--user", default="postgres", help="Database user")
    parser.add_argument("--password", default="", help="Database password")
    parser.add_argument("--datadir", default="data", help="Directory containing CSV files")
    args = parser.parse_args()

    try:
        conn = psycopg2.connect(
            host=args.host,
            port=args.port,
            dbname=args.dbname,
            user=args.user,
            password=args.password,
        )
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

    print(f"Connected to {args.dbname}@{args.host}")
    cur = conn.cursor()


    execute_sql_file(cur, "schema.sql")
    conn.commit()

 
    cur.execute("TRUNCATE TABLE stop_events, trips, line_stops, stops, lines RESTART IDENTITY CASCADE;")
    conn.commit()

    total_rows = 0

    total_rows += load_lines(cur, args.datadir)
    conn.commit()

    total_rows += load_stops(cur, args.datadir)
    conn.commit()

    line_map = build_line_map(cur)
    stop_map = build_stop_map(cur)

    total_rows += load_line_stops(cur, args.datadir, line_map, stop_map)
    conn.commit()

    total_rows += load_trips(cur, args.datadir, line_map)
    conn.commit()

    total_rows += load_stop_events(cur, args.datadir, stop_map)
    conn.commit()

   

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()


