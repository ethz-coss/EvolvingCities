DROP TABLE IF EXISTS "{{ params.time_consistent_cluster_industry_table }}";
DROP TABLE IF EXISTS "{{ params.time_consistent_cluster_table }}";
DROP TABLE IF EXISTS "{{ params.time_consistent_cluster_geometry_table }}";

-- Create the time consistent cluster geometry table
CREATE TABLE "{{ params.time_consistent_cluster_geometry_table }}" AS
WITH multiyear_cluster_with_uid AS (
    SELECT m.cluster_uid, c.year, c.cluster_id, population, geom
    FROM "{{ params.crosswalk_cluster_uid_to_cluster_id_table }}" m JOIN "{{ params.multiyear_cluster_table }}" c
    ON m.cluster_id = c.cluster_id AND m.year = c.year
    )
SELECT cluster_uid, ST_Union(geom) AS geom
FROM multiyear_cluster_with_uid
GROUP BY cluster_uid;

ALTER TABLE "{{ params.time_consistent_cluster_geometry_table }}" ADD PRIMARY KEY (cluster_uid);
CREATE INDEX ON "{{ params.time_consistent_cluster_geometry_table }}" USING GIST (geom);

-- Create the time consistent cluster and time consistent cluster industry tables
CREATE TEMPORARY TABLE cluster_uid_and_census_place_industry_count ON COMMIT DROP AS
WITH cluster_uid_census_place_crosswalk AS (
    SELECT cluster_uid, id AS census_place_id
    FROM "{{ params.time_consistent_cluster_geometry_table }}" tcc_geom JOIN "{{ params.census_place_table }}" cp
    ON ST_Within(ST_Transform(cp.geom::geometry, 5070), tcc_geom.geom)
)
SELECT cluster_uid, mcp.census_place_id, year, ind1950, worker_count
FROM cluster_uid_census_place_crosswalk cw
JOIN "{{ params.multiyear_census_place_industry_count_table }}" mcp
ON cw.census_place_id = mcp.census_place_id;

CREATE TABLE "{{ params.time_consistent_cluster_table }}" AS
SELECT cluster_uid, year, SUM(worker_count) AS population
FROM cluster_uid_and_census_place_industry_count
GROUP BY cluster_uid, year
ORDER BY cluster_uid, year;

ALTER TABLE "{{ params.time_consistent_cluster_table }}" ADD PRIMARY KEY (cluster_uid, year);
ALTER TABLE "{{ params.time_consistent_cluster_table }}" ADD FOREIGN KEY (cluster_uid) REFERENCES "{{ params.time_consistent_cluster_geometry_table }}"(cluster_uid);

CREATE TABLE "{{ params.time_consistent_cluster_industry_table }}" AS
SELECT cluster_uid, year, ind1950, SUM(worker_count) AS worker_count
FROM cluster_uid_and_census_place_industry_count
GROUP BY cluster_uid, year, ind1950
ORDER BY cluster_uid, year, ind1950;

ALTER TABLE "{{ params.time_consistent_cluster_industry_table }}" ADD PRIMARY KEY (cluster_uid, year, ind1950);
ALTER TABLE "{{ params.time_consistent_cluster_industry_table }}" ADD FOREIGN KEY (cluster_uid, year) REFERENCES "{{ params.time_consistent_cluster_table }}"(cluster_uid, year);
ALTER TABLE "{{ params.time_consistent_cluster_industry_table }}" ADD FOREIGN KEY (ind1950) REFERENCES "{{ params.industry_table }}"(code);