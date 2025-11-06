DROP TABLE IF EXISTS stop_events CASCADE;
DROP TABLE IF EXISTS trips CASCADE;
DROP TABLE IF EXISTS line_stops CASCADE;
DROP TABLE IF EXISTS stops CASCADE;
DROP TABLE IF EXISTS lines CASCADE;

CREATE TABLE lines(
    line_id SERIAL PRIMARY KEY, 
    line_name VARCHAR(50) NOT NULL UNIQUE,
    vehicle_type VARCHAR(10) NOT NUll
        CHECK (vehicle_type IN ('rail','bus'))
);

CREATE TABLE stops(
    stop_id SERIAL PRIMARY KEY,
    stop_name VARCHAR(50) NOT NULL UNIQUE,
    latitude NUMERIC(9,6) NOT NULL,
    longitude NUMERIC(9,6) NOT NULL
);

CREATE TABLE line_stops (
    line_id INTEGER NOT NULL REFERENCES lines(line_id) ON DELETE CASCADE,
    stop_id INTEGER NOT NULL REFERENCES stops(stop_id) ON DELETE CASCADE,
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    time_offset INTEGER NOT NULL CHECK (time_offset >= 0),
    PRIMARY KEY (line_id, stop_id),
    UNIQUE (line_id, sequence)
);

CREATE TABLE trips (
    trip_id VARCHAR(10) PRIMARY KEY,
    line_id INTEGER NOT NULL REFERENCES lines(line_id) ON DELETE CASCADE,
    scheduled_departure TIMESTAMP NOT NULL,
    vehicle_id VARCHAR(5) NOT NULL
);

CREATE TABLE stop_events (
    event_id SERIAL PRIMARY KEY,
    trip_id VARCHAR(20) NOT NULL REFERENCES trips(trip_id) ON DELETE CASCADE,
    stop_id INTEGER NOT NULL REFERENCES stops(stop_id) ON DELETE CASCADE,
    scheduled TIMESTAMP NOT NULL,
    actual TIMESTAMP NOT NULL,
    passengers_on INTEGER NOT NULL CHECK (passengers_on >= 0),
    passengers_off INTEGER NOT NULL CHECK (passengers_off >= 0),
    UNIQUE (trip_id, stop_id, scheduled)
);