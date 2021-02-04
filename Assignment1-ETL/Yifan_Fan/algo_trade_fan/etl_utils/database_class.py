"""
This script is to initialize the AWS RDS PostgreSQL database,
as well as store modification functions to manage the database
"""

import pandas as pd
import psycopg2
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Column, String, Float, DateTime, Integer, BigInteger, Date, Table
from etl_utils.etl_config import RDS_CONFIG


class RemoteDatabase:
    """
    This class is to manage the connection of AWS RDS PostgreSQL database
    """

    def __init__(self, tb_name, user_name, password, endpoint, db_name=RDS_CONFIG["DATABASE"]):
        self.tb_name, self.user_name, self.password, self.endpoint, self.db_name = \
            tb_name, user_name, password, endpoint, db_name
        DB_URL = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(user_name, password, endpoint,
                                                                    RDS_CONFIG["PORT"], db_name)
        self.engine = create_engine(DB_URL)
        self.metadata = MetaData(self.engine)
        # check if the table exists:
        if self.tb_name in self.engine.table_names():
            self.current_table = Table(self.tb_name, self.metadata, autoload=True)
        else:  # if no such table, ask to create:
            print('The accessing table do not exists')
            build_tb = input('input "yes" to create [{}]: '.format(self.tb_name))
            if build_tb == "yes":
                self._create_table()
            else:
                raise Exception("You have to create a table before access.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # No connection build during the process
        pass

    def stack_list(self):
        """
        This method is to pre-load the existed stack list for checking new stacks.

        :return: (DataFrame)
        """
        print("Preparing Stack List of {}... ".format(self.tb_name))
        with self.engine.connect() as con:
            sql_query = "SELECT a.symbol, a.last_time, b.volume FROM " \
                        "(SELECT symbol, MAX(timestamp) AS last_time FROM {t} GROUP BY symbol) a " \
                        "LEFT JOIN " \
                        "(SELECT symbol, timestamp, volume FROM {t}) b " \
                        "ON a.last_time = b.timestamp AND a.symbol = b.symbol".format(t=self.tb_name)
            rs = con.execute(sql_query).fetchall()
            rs = pd.DataFrame(rs)
            rs.columns = ['symbol', 'last_time', 'volume']
            return rs

    def _create_table(self):
        """
        Build three main tables to save 'daily' candles, 'intraday minute level' candles, and 'splits' information.
        """
        if self.tb_name in self.engine.table_names():
            print("The table [{}] has already been created, please drop it at first.".format(self.tb_name))
            return None
        else:
            print("----- CREATING TABLE [{}] -----".format(self.tb_name))
        # Check if the table are pre-defined table:
        if self.tb_name in [RDS_CONFIG['DAILY_TABLE'], RDS_CONFIG['INTRADAY_TABLE']]:
            self.current_table = Table(self.tb_name, self.metadata,
                                       Column('close_price', Float(5)),
                                       Column('high_price', Float(5)),
                                       Column('low_price', Float(5)),
                                       Column('open_price', Float(5)),
                                       Column('status', String(255)),
                                       Column('timestamp', DateTime(timezone=True),
                                              default=datetime.utcnow),
                                       Column('volume', BigInteger()),
                                       Column('symbol', String(255)))
        elif self.tb_name == RDS_CONFIG['SPLIT_TABLE']:
            self.current_table = Table(self.tb_name, self.metadata,
                                       Column('symbol', String(255), primary_key=True),
                                       Column('date', Date(), primary_key=True),
                                       Column('fromFactor', Integer()),
                                       Column('toFactor', Integer()),
                                       Column('source', String(255)))
        else:
            print("Sorry creating table {} is not supported for now.".format(self.tb_name))
        # Build the table:
        self.metadata.create_all(self.engine)
        # To check if the table is successful created:
        print("----- TABLE [{}] CREATED -----".format(self.tb_name))

    def info(self):
        """
        Print out the database column information.
        """
        print(self.current_table.columns.keys())

    def _drop_table(self):
        """
        Drop the current table.
        """
        print("----- DROP TABLE [{}] -----".format(self.tb_name))
        self.current_table.drop(self.engine)
        print("----- TABLE [{}] DOPED -----".format(self.tb_name))

    def update_dataframe(self, df):
        """
        :param df: (DataFrame)
        :return: None. Only process operations in database.
        """
        # build the connection to bulk insert data.
        if df.empty:
            print("Nothing to upload.")
            return None
        # Build Bulk insert connection to upload:
        up_con = psycopg2.connect(database=self.db_name, user=self.user_name,
                                  password=self.password, host=self.endpoint, port=RDS_CONFIG["PORT"])
        tmp_df = "./tmp_dataframe.csv"
        df.to_csv(tmp_df, index=False, header=False)
        f = open(tmp_df, 'r')
        cursor = up_con.cursor()
        try:
            cursor.copy_from(f, self.tb_name, sep=",", size=RDS_CONFIG["CHUNK_SIZE"])
            up_con.commit()
            cursor.close()
            up_con.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            up_con.rollback()
            cursor.close()
            up_con.close()
            return None

    def _get_volume(self, symbol, dt_select):
        """
        Return the selected candles records by conditions.

        :param symbol: (str) Stack abbreviation
        :param dt_select: (datetime)
        :return: (Integer) The corresponding volume
        """
        # Get all result
        with self.engine.connect() as con:
            query = "SELECT volume FROM {} WHERE symbol = '{}' and timestamp = '{}'" \
                .format(self.tb_name, symbol,
                        dt_select.strftime("%Y-%m-%d %H:%M:%S"))
            rs = con.execute(query).scalar()
            return rs

    def split_info(self, symbol, from_date, to_date):
        """
        Get the corresponding split information in the selected period.

        :param symbol:(str) Stack abbreviation
        :param from_date: (date)
        :param to_date: (date)
        :return: (DataFrame) None if not found.
        """
        try:
            split_tb = Table(RDS_CONFIG["SPLIT_TABLE"], self.metadata, autoload=True)
        except Exception as e:
            raise Exception('Please create "split_raw" table at first.')
        with self.engine.connect() as con:
            query = "SELECT * FROM {} WHERE symbol = '{}' and date BETWEEN '{}' and '{}';" \
                .format(split_tb, symbol,
                        from_date.strftime("%Y-%m-%d"),
                        to_date.strftime("%Y-%m-%d"))
            rs = con.execute(query).fetchall()
            rs = pd.DataFrame(rs)
            rs.columns = ['symbol', 'date', 'fromFactor', 'toFactor', 'source']
            return rs

    def delete_stack(self, symbol):
        """
        Delete all the records of the stack in table.

        :param symbol: (str) Stack abbreviation
        :return: None. Only process operations in database.
        """
        print("Delete all records of {} from {}".format(symbol, self.tb_name))
        with self.engine.connect() as con:
            query = "DELETE FROM {} WHERE symbol = '{}'".format(self.tb_name, symbol)
            res = con.execute(query)
            print("{} rows has been deleted.".format(res.rowcount))
