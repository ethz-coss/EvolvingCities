DROP TABLE IF EXISTS "{{ params.cluster_table }}";

-- Create a binary raster from the smod raster with only urban and non-urban classes
CREATE TEMPORARY TABLE smod_binary ON COMMIT DROP AS
SELECT ST_Reclass({{ params.smod_table }}.rast, 1,  '[0-{{ params.lower_bound_urban }}]\:0, ({{ params.lower_bound_urban }}-30]\:1', '1BB', nodataval := 0) AS rast
FROM {{ params.smod_table }};

-- Create a temporary cluster geometry table with DBSCAN
CREATE TEMPORARY TABLE cluster_geom ON COMMIT DROP AS
WITH urban_pixels AS (
    SELECT (ST_PixelAsPolygons(rast, 1, TRUE)).*
    FROM smod_binary
),
dbscan AS (
    SELECT ST_ClusterDBSCAN(geom, eps := {{ params.dbscan_eps }}, minpoints := {{ params.dbscan_minpoints }}) OVER () AS cluster_id, geom
    FROM urban_pixels
)
SELECT cluster_id, ST_Union(geom) AS geom
FROM dbscan
GROUP BY cluster_id;

-- Create the cluster table with the geometry and population
CREATE TABLE {{ params.cluster_table }} AS
WITH zonal_stats AS (
    SELECT cluster_id, (St_SummaryStats(St_Union(ST_Clip(rast, 1, geom, true)))).*
    FROM cluster_geom, {{ params.pop_table }}
    WHERE St_Intersects(rast,geom)
    GROUP BY cluster_id
)
SELECT cluster_id, sum AS population, geom
FROM zonal_stats JOIN cluster_geom USING(cluster_id);