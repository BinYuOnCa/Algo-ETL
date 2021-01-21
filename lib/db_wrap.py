from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base

from util.config import global_config
import secrets


# host = "algodb.cnnftusbadxi.ca-central-1.rds.amazonaws.com"
# user = "postgres"
# password = "algodsclass"
# db = "algo"

db_params = dict(
    host=global_config['db']['host'],
    user=global_config['db']['user'],
    password=secrets.DB_PASSWORD,
    db=global_config['db']['db_name'],
)

_engine = None
_metadata = None
_base = None

def get_engine():
    global _engine
    if _engine is None:
        _connect_db()
    return _engine


def truncate(table_name):
    if _engine is None:
        _connect_db()
    if _metadata.tables.get(table_name) is not None:
        _engine.execute(f'DELETE FROM "{table_name}"')
    else:
        print(f'Truncate: cannot find table "{table_name}"')


def _connect_db():
    global _engine, _metadata, _base
    alchemy_engine = "postgresql://{}:{}@{}:5432/{}".format(
        db_params['user'], db_params['password'], db_params['host'], db_params['db'])
    _engine = create_engine(alchemy_engine)
    _metadata = MetaData(_engine, reflect=True)
    _base = declarative_base()
    return _engine

def append_to_db(conn, df, table_name, if_cache=False):
    if not if_cache:
        df.to_sql(table_name, conn, if_exists='append', method='multi')
