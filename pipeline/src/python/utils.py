from typing import Dict, List
import subprocess
import os
from enum import Enum
from sqlalchemy import create_engine, text, MetaData, Table
import functools
import jinja2
import logging
from config import config


logger = logging.getLogger('cluster_pipeline')
logger.setLevel(logging.DEBUG if config.debug else logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

engine_temp_duckdb = create_engine(config.db.temp_duckdb_uri, echo=True if config.debug else False)
engine_ipums_postgres = create_engine(config.db.ipums_postgres_uri, echo=True if config.debug else False)
metadata_ipums_postgres = MetaData()
engine_ghsl_postgres = create_engine(config.db.ghsl_postgres_uri, echo=True if config.debug else False)
metadata_ghsl_postgres = MetaData()


class DB(Enum):
    TEMP_DUCKDB = 'temp_duckdb'
    IPUMS_POSTGRES = 'clusterdb_postgres'
    GHSL_POSTGRES = 'ghsl_postgres'


def run_sql_script_on_db(db: DB):
    def decorator_run_sql_script_on_db(func):
        @functools.wraps(func)
        def wrapper_sql_script_on_db(*args, **kwargs):
            e = get_db_engine(db)
            sql_file_path, params = func(*args, **kwargs)
            with e.begin() as conn:
                execute_sql_file(conn=conn,
                                 file_path=sql_file_path,
                                 params=params)

        return wrapper_sql_script_on_db
    return decorator_run_sql_script_on_db


def get_db_engine(db: DB):
    if db == DB.TEMP_DUCKDB:
        return engine_temp_duckdb
    elif db == DB.IPUMS_POSTGRES:
        return engine_ipums_postgres
    elif db == DB.GHSL_POSTGRES:
        return engine_ghsl_postgres
    else:
        raise ValueError(f"Database {db.value} not supported")


def get_postgres_table(name: str, db: DB):
    if db == DB.IPUMS_POSTGRES:
        return Table(name, metadata_ipums_postgres, autoload_with=engine_ipums_postgres)
    elif db == DB.GHSL_POSTGRES:
        return Table(name, metadata_ghsl_postgres, autoload_with=engine_ghsl_postgres)
    else:
        raise ValueError(f"Database {db.value} not supported")


def execute_sql_file(conn, file_path: str, params: Dict[str, str] = None):
    with open(file_path) as f:
        sql = f.read()

    if params is None:
        params = {}

    template = jinja2.Template(sql)
    sql = template.render(params=params)
    conn.execute(text(sql))


def execute_bash_script(file_path: str, args: List[str]):
    # Change the permissions of the script to make it executable
    os.chmod(file_path, 0o755)

    print("Running bash script with arguments:")
    # Run the bash script with arguments using subprocess.Popen
    process = subprocess.Popen([file_path] + args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Read and print the output live
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            print(output.strip())

    # Print any errors
    err = process.stderr.read()
    if err:
        print("stderr:", err.strip())

    # Print the return code
    print("Return code:", process.returncode)

