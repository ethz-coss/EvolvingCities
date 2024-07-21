-- Drop tables if they exist for idempotency
DROP TABLE IF EXISTS "{{ params.census_table_name }}";
DROP TABLE IF EXISTS "{{ params.census_place_industry_count_table_name }}";

-- Create census table
CREATE TABLE "{{ params.census_table_name }}"
    (histid VARCHAR(36),
     hik VARCHAR(21),
     ind1950 INTEGER,
     occ1950 INTEGER,
     census_place_id INTEGER);

-- Merge the data from the demographic and geographic tables
INSERT INTO "{{ params.census_table_name }}"
SELECT "{{ params.dem_table_name }}".histid, NULLIF(hik, '                     '), ind1950, occ1950, CASE WHEN census_place_id > 69491 THEN NULL ELSE census_place_id END AS census_place_id
FROM "{{ params.dem_table_name }}" LEFT JOIN "{{ params.geo_table_name }}"
ON "{{ params.dem_table_name }}".histid = "{{ params.geo_table_name }}".histid;

-- Create census place industry count table
CREATE TABLE "{{ params.census_place_industry_count_table_name }}" AS
SELECT census_place_id, ind1950, COUNT(*) AS worker_count
FROM "{{ params.census_table_name }}"
WHERE census_place_id IS NOT NULL
GROUP BY census_place_id, ind1950;