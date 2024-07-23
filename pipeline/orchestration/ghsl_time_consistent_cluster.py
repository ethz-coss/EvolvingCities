from sqlalchemy import text
from src.python.utils import execute_bash_script, run_sql_script_on_db, DB, get_db_engine
from src.python.multi_year_matching import get_cluster_year_connected_component_table
from common import create_multiyear_table as _create_multiyear_table, create_crosswalk_cluster_uid_to_cluster_id as _create_crosswalk_cluster_uid_to_cluster_id, create_cluster_intersection_matching as _create_cluster_intersection_matching
from config import config


def load_ghsl_rasters():
    for year in config.param.ghsl.years:
        _load_ghsl_rasters(year=year)


def _load_ghsl_rasters(year: int):
    args = [str(year), config.path.source_data.pop.format(year=year), config.path.source_data.smod.format(year=year),
            config.db.ghsl_table.pop.format(year=year), config.db.ghsl_table.smod.format(year=year),
            config.db.postgres_user, config.db.postgres_password, config.db.postgres_host, str(config.db.postgres_port), config.db.ghsl_postgres_db_name]
    execute_bash_script(file_path=config.path.bash.ghsl_etl.load_ghsl_rasters, args=args)


def create_cluster():
    for year in config.param.ghsl.years:
        _create_cluster(year=year)


@run_sql_script_on_db(db=DB.GHSL_POSTGRES)
def _create_cluster(year: int):
    sql_file_path = config.path.sql.ghsl_tcc.create_cluster
    params = {
        'pop_table': config.db.ghsl_table.pop.format(year=year),
        'smod_table': config.db.ghsl_table.smod.format(year=year),
        'cluster_table': config.db.ghsl_table.cluster.format(year=year),
        'lower_bound_urban': config.param.ghsl.lower_bound_urban,
        'dbscan_eps': config.param.ghsl.dbscan_eps,
        'dbscan_minpoints': config.param.ghsl.dbscan_min_points
    }
    return sql_file_path, params


def create_multiyear_tables_and_cluster_intersection_matching():
    _create_multiyear_table(base_table_name=config.db.ghsl_table.cluster,
                            multiyear_cluster_table_name=config.db.ghsl_table.multiyear_cluster,
                            column_names=['cluster_id', 'population', 'geom'],
                            years=config.param.ghsl.years,
                            create_spatial_index=True,
                            db=DB.GHSL_POSTGRES)

    _create_cluster_intersection_matching(db=DB.GHSL_POSTGRES,
                                          cluster_intersection_matching_table_name=config.db.ghsl_table.cluster_intersection_matching,
                                          multiyear_cluster_table_name=config.db.ghsl_table.multiyear_cluster)


def create_crosswalk_cluster_uid_to_cluster_id() -> None:
    _create_crosswalk_cluster_uid_to_cluster_id(db=DB.GHSL_POSTGRES,
                                                intersection_matching_table_name=config.db.ghsl_table.cluster_intersection_matching,
                                                crosswalk_cluster_uid_to_cluster_id_table_name=config.db.ghsl_table.crosswalk_cluster_uid_to_cluster_id)


@run_sql_script_on_db(db=DB.GHSL_POSTGRES)
def create_time_consistent_cluster_pre_geocoding():
    sql_file_path = config.path.sql.ghsl_tcc.create_time_consistent_cluster
    params = {
        'multiyear_cluster_table': config.db.ghsl_table.multiyear_cluster,
        'crosswalk_cluster_uid_to_cluster_id_table': config.db.ghsl_table.crosswalk_cluster_uid_to_cluster_id,
        'time_consistent_cluster_pre_geocoding_table': config.db.ghsl_table.time_consistent_cluster_pre_geocoding,
        'time_consistent_cluster_geometry_pre_geocoding_table': config.db.ghsl_table.time_consistent_cluster_geometry_pre_geocoding
    }
    return sql_file_path, params


def load_country_borders():
    e = get_db_engine(db=DB.GHSL_POSTGRES)

    with e.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {config.db.ghsl_table.country_borders}"))
        conn.execute(text(f"DROP TABLE IF EXISTS {config.db.ghsl_table.crosswalk_cshape_to_world_bank_codes}"))

    args = [config.path.source_data.country_borders, config.db.ghsl_table.country_borders,
            config.path.source_data.crosswalk_cshape_to_world_bank_codes, config.db.ghsl_table.crosswalk_cshape_to_world_bank_codes,
            config.db.postgres_user, config.db.postgres_password, config.db.postgres_host, str(config.db.postgres_port), config.db.ghsl_postgres_db_name]
    execute_bash_script(file_path=config.path.bash.ghsl_etl.load_country_borders, args=args)


@run_sql_script_on_db(db=DB.GHSL_POSTGRES)
def geocode_cluster_with_country():
    sql_file_path = config.path.sql.ghsl_tcc.country_geocoding
    params = {
        'time_consistent_cluster_pre_geocoding_table': config.db.ghsl_table.time_consistent_cluster_pre_geocoding,
        'time_consistent_cluster_geometry_pre_geocoding_table': config.db.ghsl_table.time_consistent_cluster_geometry_pre_geocoding,
        'time_consistent_cluster_table': config.db.ghsl_table.time_consistent_cluster,
        'country_borders_table': config.db.ghsl_table.country_borders,
        'time_consistent_cluster_geometry_table': config.db.ghsl_table.time_consistent_cluster_geometry
    }
    return sql_file_path, params



if __name__ == '__main__':
    load_country_borders()