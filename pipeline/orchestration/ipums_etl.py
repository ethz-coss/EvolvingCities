import pandas as pd
import sqlalchemy
from src.python.utils import run_sql_script_on_db, DB, get_db_engine, logger
from config import config


def configure_duckdb():
    e = get_db_engine(db=DB.TEMP_DUCKDB)
    with e.begin() as conn:
        conn.execute(sqlalchemy.text("SET max_memory='40GB';"))
        conn.execute(sqlalchemy.text("SET enable_progress_bar = false;"))


def extract_data_to_duckdb():
    for y in config.param.ipums.years:
        logger.debug(f"Extracting data for year {y}")
        _extract_data_to_duckdb(y)


@run_sql_script_on_db(db=DB.TEMP_DUCKDB)
def _extract_data_to_duckdb(y: int):
    sql_file_path = config.path.sql.ipums_etl.extract
    params = {
        'dem_table_name': config.db.ipums_table.dem.format(year=y),
        'geo_table_name': config.db.ipums_table.geo.format(year=y),
        'dem_file_name': config.path.source_data.dem.format(year=y),
        'geo_file_name': config.path.source_data.geo.format(year=y)
    }
    return sql_file_path, params


def transform_data():
    for y in config.param.ipums.years:
        logger.debug(f"Transforming data for year {y}")
        _transform_data(y)


@run_sql_script_on_db(db=DB.TEMP_DUCKDB)
def _transform_data(y: int):
    sql_file_path = config.path.sql.ipums_etl.transform
    params = {
        'dem_table_name': config.db.ipums_table.dem.format(year=y),
        'geo_table_name': config.db.ipums_table.geo.format(year=y),
        'census_table_name': config.db.ipums_table.census.format(year=y),
        'census_place_industry_count_table_name': config.db.ipums_table.census_place_industry_count.format(year=y)
    }
    return sql_file_path, params


def load_data_to_postgres():
    _load_census_place_and_industry_code_tables_to_postgres()
    for y in config.param.ipums.years:
        copy_table_from_duckdb_to_postgres(table_name=config.db.ipums_table.census_place_industry_count.format(year=y))


@run_sql_script_on_db(db=DB.IPUMS_POSTGRES)
def _load_census_place_and_industry_code_tables_to_postgres():
    sql_file_path = config.path.sql.ipums_etl.load
    params = {
        'census_place_table_name': config.db.ipums_table.census_place,
        'industry_code_table_name': config.db.ipums_table.industry_code,
        'census_place_file_name': config.path.source_data.census_place,
        'industry_code_file_name': config.path.source_data.industry_code
    }
    return sql_file_path, params


def copy_table_from_duckdb_to_postgres(table_name: str, chunksize: int = 10000):
    e_duckdb = get_db_engine(db=DB.TEMP_DUCKDB)
    e_postgres = get_db_engine(db=DB.IPUMS_POSTGRES)

    # Drop table for idempotency
    with e_postgres.begin() as conn:
        conn.execute(sqlalchemy.text(f'DROP TABLE IF EXISTS {table_name}'))

    for chunk in pd.read_sql_query(f'SELECT * FROM {table_name}', e_duckdb, chunksize=chunksize):
        chunk.to_sql(table_name, e_postgres, if_exists='append', index=False)