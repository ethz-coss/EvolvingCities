DROP TABLE IF EXISTS "{{ params.time_consistent_cluster_pre_geocoding_table }}";
DROP TABLE IF EXISTS "{{ params.time_consistent_cluster_geometry_pre_geocoding_table }}";

-- Create the time consistent cluster geometry table
CREATE TABLE "{{ params.time_consistent_cluster_geometry_pre_geocoding_table }}" AS
WITH multiyear_cluster_with_uid AS (
    SELECT m.cluster_uid, c.year, c.cluster_id, geom
    FROM "{{ params.crosswalk_cluster_uid_to_cluster_id_table }}" m JOIN "{{ params.multiyear_cluster_table }}" c
    ON m.cluster_id = c.cluster_id AND m.year = c.year
)
SELECT cluster_uid, ST_Union(geom) AS geom
FROM multiyear_cluster_with_uid
GROUP BY cluster_uid;

CREATE INDEX ON "{{ params.time_consistent_cluster_geometry_pre_geocoding_table }}" USING GIST (geom);

-- Create the time consistent cluster table
CREATE TABLE "{{ params.time_consistent_cluster_pre_geocoding_table }}" AS
WITH multiyear_cluster_with_uid AS (
    SELECT m.cluster_uid, c.year, c.cluster_id, population
    FROM "{{ params.crosswalk_cluster_uid_to_cluster_id_table }}" m JOIN "{{ params.multiyear_cluster_table }}" c
    ON m.cluster_id = c.cluster_id AND m.year = c.year
)
SELECT cluster_uid, year, SUM(population) AS population
FROM multiyear_cluster_with_uid
GROUP BY cluster_uid, year;
