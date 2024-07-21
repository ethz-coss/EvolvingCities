DROP TABLE IF EXISTS "{{ params.cluster_industry_table }}";
DROP TABLE IF EXISTS "{{ params.cluster_table }}";

-- Create a temporary table to store the cluster geometries
CREATE TEMPORARY TABLE cluster_geom_tmp ON COMMIT DROP AS
WITH pixels AS (
        SELECT (ST_PixelAsPolygons(rast, 1, TRUE)).*
        FROM "{{ params.convolved_census_place_raster_table }}"
    ),
    filtered_pixel AS (
        SELECT val, geom
        FROM pixels
        WHERE val > {{ params.pixel_threshold }}
    ),
    dbscan AS (
        SELECT ST_ClusterDBSCAN(geom, eps := {{ params.dbscan_eps }}, minpoints := {{ params.dbscan_minpoints }}) OVER () AS cid, geom, val
        FROM filtered_pixel
    )
SELECT cid AS cluster_id, ST_Union(geom) AS geom
FROM dbscan
GROUP BY cid;

CREATE INDEX ON cluster_geom_tmp USING GIST (geom);

-- Create a temporary table to store the cluster to census place crosswalk
CREATE TEMPORARY TABLE cluster_census_place_crosswalk ON COMMIT DROP AS
SELECT id AS census_place_id, cluster_id
FROM cluster_geom_tmp JOIN "{{ params.census_place_table }}"
ON ST_Within(ST_Transform("{{ params.census_place_table }}".geom::geometry, 5070), cluster_geom_tmp.geom);

-- Create a temporary table to store the cluster population
CREATE TEMPORARY TABLE cluster_pop_tmp ON COMMIT DROP AS
WITH population_census_place AS (
        SELECT census_place_id, SUM(worker_count) AS population
        FROM "{{ params.census_place_industry_count_table }}"
        GROUP BY census_place_id
    )
SELECT cc.cluster_id, SUM(pcc.population) AS population
FROM population_census_place pcc JOIN cluster_census_place_crosswalk cc
ON pcc.census_place_id = cc.census_place_id
GROUP BY cluster_id;

-- Create the cluster table
CREATE TABLE "{{ params.cluster_table }}" AS
SELECT cluster_geom_tmp.cluster_id, population, geom
FROM cluster_geom_tmp JOIN cluster_pop_tmp
ON cluster_geom_tmp.cluster_id = cluster_pop_tmp.cluster_id;

CREATE INDEX ON "{{ params.cluster_table }}" USING GIST (geom);
ALTER TABLE "{{ params.cluster_table }}" ADD PRIMARY KEY (cluster_id);

-- Create the cluster industry table
CREATE TABLE "{{ params.cluster_industry_table }}" AS
SELECT cluster_id, ind1950, SUM(worker_count) AS worker_count
FROM "{{ params.census_place_industry_count_table }}" AS industry
JOIN cluster_census_place_crosswalk AS crosswalk
ON industry.census_place_id = crosswalk.census_place_id
GROUP BY cluster_id, ind1950
ORDER BY cluster_id, ind1950;

ALTER TABLE "{{ params.cluster_industry_table }}" ADD PRIMARY KEY (cluster_id, ind1950);
ALTER TABLE "{{ params.cluster_industry_table }}" ADD FOREIGN KEY (cluster_id) REFERENCES "{{ params.cluster_table }}"(cluster_id);
ALTER TABLE "{{ params.cluster_industry_table }}" ADD FOREIGN KEY (ind1950) REFERENCES "{{ params.industry_table }}"(code);

