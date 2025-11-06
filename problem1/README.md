1. Schema Decisions: Natural vs surrogate keys? Why?

Used integer surrogate keys (e.g., line_id, stop_id) as primary keys for stability and efficiency.
Line and stop names have UNIQUE constraints to ensure logical uniqueness.

2. Constraints: What CHECK/UNIQUE constraints did you add?

UNIQUE: line_name, stop_name, (line_id, stop_id)
NOT NULL: critical fields such as stop_name, latitude, and longitude.
No explicit CHECK constraints were implemented, but latitude and longitude are defined as NUMERIC(9,6) to maintain precision and prevent invalid values during data loading.
These constraints ensure data uniqueness, completeness, and consistency.

3. Complex Query: Which query was hardest? Why?

The hardest query was Q9, counting delayed stops per trip.
It required comparing scheduled and actual times and grouping by trip_id, involving both time comparison and aggregation.

4. Foreign Keys: Give example of invalid data they prevent

Foreign keys prevent invalid references.
For example, a stop_id in stop_events must exist in stops; otherwise, insertion fails, preserving referential integrity.

5. When Relational: Why is SQL good for this domain?

The metro transit system data is structured and highly relational.
SQL enforces strong consistency, supports joins and aggregations, and efficiently handles analytical queries—ideal for this domain.