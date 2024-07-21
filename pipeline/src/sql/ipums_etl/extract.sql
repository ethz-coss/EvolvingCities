-- Drop tables if they exist for idempotency
DROP TABLE IF EXISTS "{{ params.dem_table_name }}";
DROP TABLE IF EXISTS "{{ params.geo_table_name }}";

-- Create demographic data table
CREATE TABLE "{{ params.dem_table_name }}"
    (year INTEGER,
     occ1950 INTEGER,
     ind1950 INTEGER,
     histid VARCHAR(36),
     hik VARCHAR(21));

COPY "{{ params.dem_table_name }}" FROM '{{ params.dem_file_name }}';

CREATE INDEX "index_{{ params.dem_table_name }}_histid" ON "{{ params.dem_table_name }}" (histid);

WITH duplicates AS (
    SELECT histid, ROW_NUMBER() OVER(PARTITION BY histid) AS rownum
    FROM "{{ params.dem_table_name }}"
    )
DELETE FROM "{{ params.dem_table_name }}" USING duplicates
WHERE "{{ params.dem_table_name }}".histid = duplicates.histid AND duplicates.rownum > 1;

-- Create geographic data table
CREATE TABLE "{{ params.geo_table_name }}"
    (potential_match VARCHAR(50),
     match_type VARCHAR(50),
     lat FLOAT,
     lon FLOAT,
     state_fips_geomatch VARCHAR(2),
     county_fips_geomatch VARCHAR(5),
     cluster_k5 INTEGER,
     cpp_placeid INTEGER,
     histid VARCHAR(36)
);
COPY "{{ params.geo_table_name }}" FROM '{{ params.geo_file_name }}';


-- Transform histid to uppercase
CREATE TABLE "{{ params.geo_table_name }}_new" (census_place_id INTEGER, histid VARCHAR(36));

INSERT INTO "{{ params.geo_table_name }}_new"
SELECT cpp_placeid, UPPER(histid) FROM "{{ params.geo_table_name }}";

DROP TABLE "{{ params.geo_table_name }}";
ALTER TABLE "{{ params.geo_table_name }}_new" RENAME TO "{{ params.geo_table_name }}";

-- Create index
CREATE INDEX "index_{{ params.geo_table_name }}_histid" ON "{{ params.geo_table_name }}" (histid);

-- Remove duplicates
WITH duplicates AS (
    SELECT histid, ROW_NUMBER() OVER(PARTITION BY histid) AS rownum
    FROM "{{ params.geo_table_name }}"
    )
DELETE FROM "{{ params.geo_table_name }}" USING duplicates
WHERE "{{ params.geo_table_name }}".histid = duplicates.histid AND duplicates.rownum > 1;