import numpy as np
from sqlalchemy import text

from src.python.utils import run_sql_script_on_db, DB, get_db_engine
from src.python.postgis_raster_io import load_raster, dump_raster
from src.python.convolution import get_2d_exponential_kernel, convolve2d
from config import config


def initialize_db():
    _clusterdb_config()
    _create_function__template_usa_raster()


def _clusterdb_config():
    e = get_db_engine(db=DB.IPUMS_POSTGRES)
    with e.begin() as conn:
        conn.execute(text("ALTER DATABASE clusterdb SET postgis.gdal_enabled_drivers = 'ENABLE_ALL';"))


@run_sql_script_on_db(db=DB.IPUMS_POSTGRES)
def _create_function__template_usa_raster():
    sql_file_path = config.path.sql.ipums_tcc.create_function__template_usa_raster
    return sql_file_path, {}


def rasterize_census_places():
    for y in config.param.ipums.years:
        _rasterize_census_places(y)


@run_sql_script_on_db(db=DB.IPUMS_POSTGRES)
def _rasterize_census_places(y: int):
    sql_file_path = config.path.sql.ipums_tcc.rasterize_census_places
    params = {
        'rasterized_census_places_table': config.db.ipums_table.rasterized_census_places.format(year=y),
        'census_place_industry_count_table': config.db.ipums_table.census_place_industry_count.format(year=y),
    }
    return sql_file_path, params


def create_convolved_census_place_raster():
    for y in config.param.ipums.years:
        _create_convolved_census_place_raster(y)


def _create_convolved_census_place_raster(y: int) -> None:
    e = get_db_engine(db=DB.IPUMS_POSTGRES)

    # Drop table for idempotency
    with e.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {config.db.ipums_table.convolved_census_place_raster.format(year=y)}"))

    with e.begin() as conn:
        raster = load_raster(con=conn, raster_table=config.db.ipums_table.rasterized_census_places.format(year=y))
        raster_vals = raster.sel(band=1).values

        kernel = get_2d_exponential_kernel(size=config.param.ipums.convolution_kernel_size, decay_rate=config.param.ipums.convolution_kernel_decay_rate)
        convolved_raster_vals = convolve2d(image=raster_vals, kernel=kernel)

        convolved_raster = raster.copy(data=np.expand_dims(convolved_raster_vals, axis=0))
        dump_raster(con=conn, data=convolved_raster, table_name=config.db.ipums_table.convolved_census_place_raster.format(year=y))


def create_cluster():
    for y in config.param.ipums.years:
        _create_cluster(y)


@run_sql_script_on_db(db=DB.IPUMS_POSTGRES)
def _create_cluster(y: int):
    sql_file_path = config.path.sql.ipums_tcc.create_cluster
    params = {
        'cluster_table': config.db.ipums_table.cluster.format(year=y),
        'cluster_industry_table': config.db.ipums_table.cluster_industry.format(year=y),
        'census_place_industry_count_table': config.db.ipums_table.census_place_industry_count.format(year=y),
        'convolved_census_place_raster_table': config.db.ipums_table.convolved_census_place_raster.format(year=y),
        'census_place_table': config.db.ipums_table.census_place,
        'industry_table': config.db.ipums_table.industry_code,
        'dbscan_eps': config.param.ipums.dbscan_eps,
        'dbscan_minpoints': config.param.ipums.dbscan_min_points,
        'pixel_threshold': config.param.ipums.pixel_threshold
    }
    return sql_file_path, params