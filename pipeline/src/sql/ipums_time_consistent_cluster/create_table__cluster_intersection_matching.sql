DROP TABLE IF EXISTS "{{ params.cluster_intersection_matching_table }}";
CREATE TABLE "{{ params.cluster_intersection_matching_table }}" AS
SELECT c1.year as y1, c1.cluster_id AS id1, c2.year AS y2, c2.cluster_id AS id2
FROM "{{ params.multiyear_cluster_table }}" c1 JOIN "{{ params.multiyear_cluster_table }}" c2
ON ST_Intersects(c1.geom, c2.geom);