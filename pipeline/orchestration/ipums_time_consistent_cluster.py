import sqlalchemy
from sqlalchemy import text
import pandas as pd

from src.python.utils import run_sql_script_on_db, DB, get_db_engine, get_clusterdb_postgres_table
from src.python.multi_year_matching import get_cluster_year_connected_component_table
from config import config


def create_multiyear_tables_and_cluster_intersection_matching():
    _create_multiyear_table(base_table_name=config.db.table_names.cluster, multiyear_table_name=config.db.table_names.multiyear_cluster,
                            column_names=['cluster_id', 'population', 'geom'], create_spatial_index=True)
    _create_cluster_intersection_matching()

    _create_multiyear_table(base_table_name=config.db.table_names.cluster_industry,
                            multiyear_table_name=config.db.table_names.multiyear_cluster_industry,
                            column_names=['cluster_id', 'ind1950', 'worker_count'], create_spatial_index=False)


def _create_multiyear_table(base_table_name, multiyear_table_name, column_names, create_spatial_index: bool):
    query = ""
    for i, y in enumerate(config.param.years):
        query_year = (f"SELECT {y} as year, {', '.join(column_names)} "
                      f"FROM {base_table_name.format(year=y)} ")

        if i < len(config.param.years) - 1:
            query_year += "UNION ALL "

        query += query_year

    e = get_db_engine(db=DB.CLUSTERDB_POSTGRES)
    with e.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {multiyear_table_name}"))
        conn.execute(text(f"CREATE TABLE {multiyear_table_name} AS ({query})"))
        if create_spatial_index:
            conn.execute(text(f"CREATE INDEX ON {multiyear_table_name} USING GIST (geom)"))


@run_sql_script_on_db(db=DB.CLUSTERDB_POSTGRES)
def _create_cluster_intersection_matching():
    sql_file_path = config.path.sql.ipums_tcc.create_cluster_intersection_matching
    params = {
        'multiyear_cluster_table': config.db.table_names.multiyear_cluster,
        'cluster_intersection_matching_table': config.db.table_names.cluster_intersection_matching
    }
    return sql_file_path, params


def create_crosswalk_cluster_uid_to_cluster_id() -> None:
    e = get_db_engine(db=DB.CLUSTERDB_POSTGRES)

    with e.connect() as conn:
        matching = pd.read_sql(f"SELECT * FROM {config.db.table_names.cluster_intersection_matching}", con=conn)

    cluster_year_connected_component = get_cluster_year_connected_component_table(intersection_matching=matching)
    cluster_year_connected_component = cluster_year_connected_component.rename(columns={"component_id": "cluster_uid", "year": "year", "cluster_id": "cluster_id"})
    cluster_year_connected_component = cluster_year_connected_component[["cluster_uid", "year", "cluster_id"]].copy()

    with e.begin() as conn:
        cluster_year_connected_component.to_sql(name=config.db.table_names.crosswalk_cluster_uid_to_cluster_id, con=conn, index=False, if_exists='replace')


@run_sql_script_on_db(db=DB.CLUSTERDB_POSTGRES)
def create_time_consistent_cluster():
    sql_file_path = config.path.sql.ipums_tcc.create_time_consistent_cluster
    params = {
        'multiyear_cluster_table': config.db.table_names.multiyear_cluster,
        'multiyear_cluster_industry_table': config.db.table_names.multiyear_cluster_industry,
        'crosswalk_cluster_uid_to_cluster_id_table': config.db.table_names.crosswalk_cluster_uid_to_cluster_id,
        'time_consistent_cluster_table': config.db.table_names.time_consistent_cluster,
        'time_consistent_cluster_industry_table': config.db.table_names.time_consistent_cluster_industry,
        'industry_table': config.db.table_names.industry_code,
    }
    return sql_file_path, params


def add_industry_rca_column():
    e = get_db_engine(db=DB.CLUSTERDB_POSTGRES)

    with e.begin() as conn:
        conn.execute(sqlalchemy.text(f"ALTER TABLE {config.db.table_names.time_consistent_cluster_industry} DROP COLUMN IF EXISTS rca"))
        conn.execute(sqlalchemy.text(f"ALTER TABLE {config.db.table_names.time_consistent_cluster_industry} ADD COLUMN rca FLOAT"))

    for y in config.param.years:
        _add_industry_rca_column(y)


def _add_industry_rca_column(y: int):
    e = get_db_engine(db=DB.CLUSTERDB_POSTGRES)

    with e.connect() as conn:
        worker_counts = pd.read_sql(f"SELECT * FROM {config.db.table_names.time_consistent_cluster_industry} WHERE year = {y}", con=conn)

    worker_counts = worker_counts.pivot(index='cluster_uid', columns='ind1950', values='worker_count').fillna(0)
    worker_counts = worker_counts.drop(columns=0)
    numerator = worker_counts.div(worker_counts.sum(axis=1), axis=0)
    denominator = worker_counts.sum(axis=0) / worker_counts.sum().sum()
    rca = numerator.div(denominator, axis=1)

    rca = rca.melt(ignore_index=False, var_name='ind1950', value_name='rca').reset_index()
    rca = rca.loc[rca['rca'] > 0].copy()
    rca = rca.rename(columns={'cluster_uid': '_cluster_uid', 'ind1950': '_ind1950'})

    table = get_clusterdb_postgres_table(name=config.db.table_names.time_consistent_cluster_industry)
    with e.begin() as conn:
        records = rca.to_dict(orient='records')
        stmt = sqlalchemy.update(table).where(table.c.year == y).where(table.c.cluster_uid == sqlalchemy.bindparam('_cluster_uid')).where(table.c.ind1950 == sqlalchemy.bindparam('_ind1950')).values(rca=sqlalchemy.bindparam('rca'))
        conn.execute(stmt, records)


if __name__ == '__main__':
    add_industry_rca_column()