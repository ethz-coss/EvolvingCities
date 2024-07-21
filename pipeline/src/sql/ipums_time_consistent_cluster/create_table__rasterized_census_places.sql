--- Create a raster with one band
--- Each pixel has value equal to the sum of the population of the census places inside that pixel
DROP TABLE IF EXISTS "{{ params.rasterized_census_places_table }}";

CREATE TABLE "{{ params.rasterized_census_places_table }}" AS
WITH usa_raster AS (
        SELECT get_template_usa_raster() AS rast
    ),
    census_place_pop AS (
        WITH census_place_pop_count AS (
            SELECT census_place_id, SUM(worker_count) AS pop_count
            FROM "{{ params.census_place_industry_count_table }}"
            GROUP BY census_place_id
        )
        SELECT
            cp_pop_count.pop_count AS pop_count,
            cp.geom AS geom
        FROM census_place_pop_count AS cp_pop_count
        JOIN census_place AS cp
        ON cp_pop_count.census_place_id = cp.id),
   census_places_geomval AS (
       SELECT ARRAY_AGG((ST_Transform(geom::geometry, 5070), pop_count::float)::geomval) AS geomvalset
       FROM census_place_pop
   )
SELECT ST_SetValues(usa_raster.rast, 1, census_places_geomval.geomvalset, FALSE) AS rast
FROM census_places_geomval
CROSS JOIN usa_raster;

SELECT AddRasterConstraints('{{ params.rasterized_census_places_table }}'::name, 'rast'::name);