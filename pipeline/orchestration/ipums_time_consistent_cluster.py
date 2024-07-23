import sqlalchemy
import pandas as pd

from src.python.utils import run_sql_script_on_db, DB, get_db_engine, get_postgres_table
from common import create_multiyear_table as _create_multiyear_table, create_crosswalk_cluster_uid_to_cluster_id as _create_crosswalk_cluster_uid_to_cluster_id, create_cluster_intersection_matching as _create_cluster_intersection_matching
from config import config


def create_multiyear_tables_and_cluster_intersection_matching():
    _create_multiyear_table(base_table_name=config.db.ipums_table.cluster, multiyear_cluster_table_name=config.db.ipums_table.multiyear_cluster,
                            column_names=['cluster_id', 'population', 'geom'], years=config.param.ipums.years, create_spatial_index=True, db=DB.IPUMS_POSTGRES)
    _create_cluster_intersection_matching(db=DB.IPUMS_POSTGRES, cluster_intersection_matching_table_name=config.db.ipums_table.cluster_intersection_matching, multiyear_cluster_table_name=config.db.ipums_table.multiyear_cluster)

    _create_multiyear_table(base_table_name=config.db.ipums_table.census_place_industry_count,
                            multiyear_cluster_table_name=config.db.ipums_table.multiyear_census_place_industry_count,
                            column_names=['census_place_id', 'ind1950', 'worker_count'], years=config.param.ipums.years, create_spatial_index=False, db=DB.IPUMS_POSTGRES)


def create_crosswalk_cluster_uid_to_cluster_id() -> None:
    _create_crosswalk_cluster_uid_to_cluster_id(db=DB.IPUMS_POSTGRES, intersection_matching_table_name=config.db.ipums_table.cluster_intersection_matching, crosswalk_cluster_uid_to_cluster_id_table_name=config.db.ipums_table.crosswalk_cluster_uid_to_cluster_id)


@run_sql_script_on_db(db=DB.IPUMS_POSTGRES)
def create_time_consistent_cluster():
    sql_file_path = config.path.sql.ipums_tcc.create_time_consistent_cluster
    params = {
        'multiyear_cluster_table': config.db.ipums_table.multiyear_cluster,
        'multiyear_census_place_industry_count_table': config.db.ipums_table.multiyear_census_place_industry_count,
        'crosswalk_cluster_uid_to_cluster_id_table': config.db.ipums_table.crosswalk_cluster_uid_to_cluster_id,
        'census_place_table': config.db.ipums_table.census_place,
        'time_consistent_cluster_table': config.db.ipums_table.time_consistent_cluster,
        'time_consistent_cluster_industry_table': config.db.ipums_table.time_consistent_cluster_industry,
        'time_consistent_cluster_geometry_table': config.db.ipums_table.time_consistent_cluster_geometry,
        'industry_table': config.db.ipums_table.industry_code,
    }
    return sql_file_path, params


def add_industry_rca_column():
    e = get_db_engine(db=DB.IPUMS_POSTGRES)

    with e.begin() as conn:
        conn.execute(sqlalchemy.text(f"ALTER TABLE {config.db.ipums_table.time_consistent_cluster_industry} DROP COLUMN IF EXISTS rca"))
        conn.execute(sqlalchemy.text(f"ALTER TABLE {config.db.ipums_table.time_consistent_cluster_industry} ADD COLUMN rca FLOAT"))

    for y in config.param.ipums.years:
        _add_industry_rca_column(y)


def _add_industry_rca_column(y: int):
    e = get_db_engine(db=DB.IPUMS_POSTGRES)

    with e.connect() as conn:
        worker_counts = pd.read_sql(f"SELECT * FROM {config.db.ipums_table.time_consistent_cluster_industry} WHERE year = {y}", con=conn)

    worker_counts = worker_counts.pivot(index='cluster_uid', columns='ind1950', values='worker_count').fillna(0)
    worker_counts = worker_counts.drop(columns=0)
    numerator = worker_counts.div(worker_counts.sum(axis=1), axis=0)
    denominator = worker_counts.sum(axis=0) / worker_counts.sum().sum()
    rca = numerator.div(denominator, axis=1)

    rca = rca.melt(ignore_index=False, var_name='ind1950', value_name='rca').reset_index()
    rca = rca.loc[rca['rca'] > 0].copy()
    rca = rca.rename(columns={'cluster_uid': '_cluster_uid', 'ind1950': '_ind1950'})

    table = get_postgres_table(name=config.db.ipums_table.time_consistent_cluster_industry, db=DB.IPUMS_POSTGRES)
    with e.begin() as conn:
        records = rca.to_dict(orient='records')
        stmt = sqlalchemy.update(table).where(table.c.year == y).where(table.c.cluster_uid == sqlalchemy.bindparam('_cluster_uid')).where(table.c.ind1950 == sqlalchemy.bindparam('_ind1950')).values(rca=sqlalchemy.bindparam('rca'))
        conn.execute(stmt, records)


if __name__ == '__main__':
    add_industry_rca_column()