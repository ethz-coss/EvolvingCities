from typing import List
from sqlalchemy import text
import pandas as pd

from src.python.utils import DB, get_db_engine
from src.python.multi_year_matching import get_cluster_year_connected_component_table


def create_multiyear_table(base_table_name: str, multiyear_cluster_table_name: str, column_names: List[str], years: List[int], create_spatial_index: bool, db: DB):
    query = ""
    for i, y in enumerate(years):
        query_year = (f"SELECT {y} as year, {', '.join(column_names)} "
                      f"FROM {base_table_name.format(year=y)} ")

        if i < len(years) - 1:
            query_year += "UNION ALL "

        query += query_year

    e = get_db_engine(db=db)

    with e.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {multiyear_cluster_table_name}"))
        conn.execute(text(f"CREATE TABLE {multiyear_cluster_table_name} AS ({query})"))

        if create_spatial_index:
            conn.execute(text(f"CREATE INDEX ON {multiyear_cluster_table_name} USING GIST (geom)"))


def create_cluster_intersection_matching(db: DB, cluster_intersection_matching_table_name: str, multiyear_cluster_table_name: str) -> None:
    e = get_db_engine(db=db)
    with e.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {cluster_intersection_matching_table_name}"))
        conn.execute(text(f"""
        CREATE TABLE {cluster_intersection_matching_table_name} AS
        SELECT c1.year as y1, c1.cluster_id AS id1, c2.year AS y2, c2.cluster_id AS id2
        FROM {multiyear_cluster_table_name} c1 JOIN {multiyear_cluster_table_name} c2
        ON ST_Intersects(c1.geom, c2.geom);
        """))


def create_crosswalk_cluster_uid_to_cluster_id(db: DB, intersection_matching_table_name: str, crosswalk_cluster_uid_to_cluster_id_table_name: str) -> None:
    e = get_db_engine(db=db)

    with e.connect() as conn:
        matching = pd.read_sql(f"SELECT * FROM {intersection_matching_table_name}", con=conn)

    cluster_year_connected_component = get_cluster_year_connected_component_table(intersection_matching=matching)
    cluster_year_connected_component = cluster_year_connected_component.rename(columns={"component_id": "cluster_uid", "year": "year", "cluster_id": "cluster_id"})
    cluster_year_connected_component = cluster_year_connected_component[["cluster_uid", "year", "cluster_id"]].copy()

    with e.begin() as conn:
        cluster_year_connected_component.to_sql(name=crosswalk_cluster_uid_to_cluster_id_table_name, con=conn, index=False, if_exists='replace')