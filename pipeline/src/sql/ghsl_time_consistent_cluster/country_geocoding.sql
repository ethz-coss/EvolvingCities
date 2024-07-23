DROP TABLE IF EXISTS {{ params.time_consistent_cluster_table }};
DROP TABLE IF EXISTS {{ params.time_consistent_cluster_geometry_table }};

-- Temporary table of transformed country borders
CREATE TEMPORARY TABLE temp_country_geom_transformed ON COMMIT DROP AS
SELECT gwcode, gwsyear, gweyear, ST_transform(the_geom, 54009) AS geom
FROM {{ params.country_borders_table }};

CREATE INDEX ON temp_country_geom_transformed USING GIST (geom);

-- Temporary table of cluster-country matching
CREATE TEMPORARY TABLE temp_cluster_country_matching ON COMMIT DROP AS
SELECT cluster_uid, year, gwcode, gwsyear, gweyear
FROM ({{ params.time_consistent_cluster_pre_geocoding_table }} JOIN {{ params.time_consistent_cluster_geometry_pre_geocoding_table }} USING (cluster_uid)) cluster
JOIN temp_country_geom_transformed country ON st_intersects(cluster.geom, country.geom)
WHERE (gwsyear < year AND year <= gweyear) OR (year = 2020 AND gweyear = 2019);

-- Temporary table of clusters that match with multiple countries (i.e., border clusters)
CREATE TEMPORARY TABLE temp_cluster_country_matching_multiple_countries ON COMMIT DROP AS
WITH countries_matched_per_cluster AS (
    SELECT cluster_uid, year, COUNT(*) AS n_matched_countries
    FROM temp_cluster_country_matching
    GROUP BY cluster_uid, year
),
clusters_matched_with_multiple_countries AS (
  SELECT cluster_uid, year, gwcode, gwsyear, gweyear
  FROM temp_cluster_country_matching JOIN (SELECT * FROM countries_matched_per_cluster WHERE n_matched_countries >= 2) USING (cluster_uid, year)
),
cluster_country_intersection_areas AS (
    SELECT cluster_uid, year, gwcode, ST_Area(ST_Intersection(cluster.geom, country.geom)) AS intersection_area
    FROM clusters_matched_with_multiple_countries JOIN {{ params.time_consistent_cluster_geometry_pre_geocoding_table }} cluster USING(cluster_uid)
    JOIN temp_country_geom_transformed country USING (gwcode, gwsyear, gweyear)
),
cluster_country_intersection_areas_ranked AS (
    SELECT cluster_uid, year, gwcode, RANK() OVER(PARTITION BY cluster_uid, year ORDER BY intersection_area DESC) AS area_rank
    FROM cluster_country_intersection_areas
)
SELECT cluster_uid, year, gwcode
FROM cluster_country_intersection_areas_ranked WHERE area_rank = 1;

-- Temporary table of clusters that match with a single country (i.e., interior clusters)
CREATE TEMPORARY TABLE temp_cluster_country_matching_single_countries ON COMMIT DROP AS
WITH countries_matched_per_cluster AS (
    SELECT cluster_uid, year, COUNT(*) AS n_matched_countries
    FROM temp_cluster_country_matching
    GROUP BY cluster_uid, year
)
SELECT cluster_uid, year, gwcode
FROM temp_cluster_country_matching JOIN countries_matched_per_cluster USING (cluster_uid, year)
WHERE n_matched_countries = 1;

-- Final table of cluster-country matching where we cleaned clustered matching with multiple countries
-- and added world bank codes
CREATE TEMPORARY TABLE temp_cluster_country_matching_clean ON COMMIT DROP AS
WITH cluster_country_matching AS (
    SELECT * FROM temp_cluster_country_matching_single_countries
    UNION ALL
    SELECT * FROM temp_cluster_country_matching_multiple_countries)
SELECT cluster_uid, year, gwcode AS cshape_code, world_bank_code
FROM cluster_country_matching JOIN crosswalk_cshape_to_world_bank_codes
ON gwcode = cshape_code;

-- We add country information to the time consistent cluster table
-- Note: by doing this we drop clusters that are matched to no country
-- These are usually small clusters on islands off the coast of a country which are too small to be counted in the country border dataset.
CREATE TABLE {{ params.time_consistent_cluster_table }} AS
SELECT cluster_uid, year, population, cshape_code, world_bank_code
FROM {{ params.time_consistent_cluster_pre_geocoding_table }} JOIN temp_cluster_country_matching_clean USING (cluster_uid, year);

-- We filter the time consistent cluster geometry table to only include clusters that have country information
CREATE TABLE {{ params.time_consistent_cluster_geometry_table }} AS
SELECT cluster_uid, geom
FROM {{ params.time_consistent_cluster_geometry_pre_geocoding_table }}
WHERE cluster_uid IN (SELECT DISTINCT cluster_uid FROM {{ params.time_consistent_cluster_table }});

ALTER TABLE "{{ params.time_consistent_cluster_geometry_table }}" ADD PRIMARY KEY (cluster_uid);
CREATE INDEX ON "{{ params.time_consistent_cluster_geometry_table }}" USING GIST (geom);

ALTER TABLE "{{ params.time_consistent_cluster_table }}" ADD PRIMARY KEY (cluster_uid, year);
ALTER TABLE "{{ params.time_consistent_cluster_table }}" ADD FOREIGN KEY (cluster_uid) REFERENCES "{{ params.time_consistent_cluster_geometry_table }}"(cluster_uid);



