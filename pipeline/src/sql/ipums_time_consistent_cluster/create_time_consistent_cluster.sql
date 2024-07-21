DROP TABLE IF EXISTS "{{ params.time_consistent_cluster_industry_table }}";
DROP TABLE IF EXISTS "{{ params.time_consistent_cluster_table }}";

CREATE TABLE "{{ params.time_consistent_cluster_table }}" AS
WITH multiyear_cluster_with_uid AS (
    SELECT m.cluster_uid, c.year, c.cluster_id, population, geom
    FROM "{{ params.crosswalk_cluster_uid_to_cluster_id_table }}" m JOIN "{{ params.multiyear_cluster_table }}" c
    ON m.cluster_id = c.cluster_id AND m.year = c.year
    )
SELECT cluster_uid, year, ST_Union(geom) AS geom, SUM(population) AS population
FROM multiyear_cluster_with_uid
GROUP BY cluster_uid, year
ORDER BY cluster_uid, year;

ALTER TABLE "{{ params.time_consistent_cluster_table }}" ADD PRIMARY KEY (cluster_uid, year);
CREATE INDEX ON "{{ params.time_consistent_cluster_table }}" USING GIST (geom);


CREATE TABLE "{{ params.time_consistent_cluster_industry_table }}" AS
WITH multiyear_cluster_industry_with_uid AS (
    SELECT m.cluster_uid, ci.year, ci.cluster_id, ci.ind1950, worker_count
    FROM "{{ params.crosswalk_cluster_uid_to_cluster_id_table }}" m JOIN "{{ params.multiyear_cluster_industry_table }}" ci
    ON m.cluster_id = ci.cluster_id AND m.year = ci.year
)
SELECT cluster_uid, year, ind1950, SUM(worker_count) AS worker_count
FROM multiyear_cluster_industry_with_uid
GROUP BY cluster_uid, year, ind1950
ORDER BY cluster_uid, year, ind1950;

ALTER TABLE "{{ params.time_consistent_cluster_industry_table }}" ADD PRIMARY KEY (cluster_uid, year, ind1950);
ALTER TABLE "{{ params.time_consistent_cluster_industry_table }}" ADD FOREIGN KEY (cluster_uid, year) REFERENCES "{{ params.time_consistent_cluster_table }}"(cluster_uid, year);
ALTER TABLE "{{ params.time_consistent_cluster_industry_table }}" ADD FOREIGN KEY (ind1950) REFERENCES "{{ params.industry_table }}"(code);