from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base
import tempfile
import pandas as pd

from util.config import global_config
import util.error
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

def get_connection():
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

def append_to_db(conn, df, table_name, if_cache=False, method='to_sql_multi'):
    '''
    method: to_sql, to_sql_multi, copy_from_file, copy_from_memory

    copy_from_file, copy_from_memory can only be used for postgresql
    '''
    # TODO if_cache is true is not implemented
    if method == 'to_sql':
        df.to_sql(table_name, conn, if_exists='append')
    elif method == 'to_sql_multi':
        df.to_sql(table_name, conn, if_exists='append', method='multi')
    elif method == 'copy_from_file':
        copy_to_db(conn, df, table_name, from_file=True)
    elif method == 'copy_from_memory':
        copy_to_db(conn, df, table_name, from_file=False)


def copy_to_db(conn, df: pd.DataFrame, table_name: str, from_file: bool = True):
    """
    Here we are going save the dataframe on disk as
    a csv file, load the csv file
    and use copy_from() to copy it to the table
    """

    POSTGRESQL_COPY_STATEMENT = """
        COPY {0} ( {1} ) FROM STDIN WITH CSV HEADER DELIMITER AS ','
    """  # {0} for tablename {1} for columns

    f = tempfile.TemporaryFile(mode='w+') if from_file else tempfile.SpooledTemporaryFile(mode='w+')  # SpooledTemporaryFile is in memory
    with f:
        df.to_csv(f, index=df.index.names[0] is not None, header=True)
        f.seek(0)
        psycopg2_conn = conn.raw_connection()
        cursor = psycopg2_conn.cursor()
        try:
            columns = list(filter(lambda x: x, df.index.names)) + list(df.columns)
            # copy_expert can read headers of csv, while copy_from_file can not.
            cursor.copy_expert(sql=POSTGRESQL_COPY_STATEMENT.format(table_name, ','.join(columns)), file=f)
            psycopg2_conn.commit()
        except Exception as e:
            psycopg2_conn.rollback()
            cursor.close()
            raise util.error.RemoteError("Fail to copy file to db: %s" % e)
        else:
            cursor.close()
