def conn_string(choice=1):
    """
    from a string for db connection from .ini
    :param choice default =1 from string for create_engine
    :param choice = any others provides string for psycopg2
    :return: a string
    """
    import configparser
    config = configparser.ConfigParser()
    config.read('settings.ini')
    db_host = config['postgresql']['host']
    db_user = config['postgresql']['user']
    db_pwd = config['postgresql']['passwd']
    db_name = config['postgresql']['name']
    db_port = config['postgresql']['port']
    if choice == 1:
        conn_string = 'postgres://' + db_user + ':' + \
            db_pwd + '@' + db_host + ':' + db_port + '/' + db_name
    else:
        conn_string = 'user=' + db_user + ' password=' + db_pwd + \
            ' host=' + db_host + ' port=' + db_port + ' dbname=' + db_name
    return conn_string


def refresh(df, tablename):
    """
    pandas dataframe store to a db table,
    first delete exist rows with same symbol and same date
    :param df:a pandas.dataframe
    :param tablename:string
    :return:
    """
    from sqlalchemy import create_engine

    db = create_engine(conn_string())  # ,fast_executemany=True
    conn = db.connect()

    # DB insert temp table
    df.to_sql(
        tablename + '_tmp',
        con=conn,
        if_exists='replace',
        index=False,
        method='multi'
    )
    trans = conn.begin()
    try:
        # delete those rows that we are going to "upsert"
        db.execute(
            'delete from ' +
            tablename +
            ' using ' +
            tablename +
            '_tmp where ' +
            tablename +
            '.symbol=' +
            tablename +
            '_tmp.symbol and ' +
            tablename +
            '.date=' +
            tablename +
            '_tmp.date')
        db.execute(
            'insert into ' +
            tablename +
            ' select * from ' +
            tablename +
            '_tmp')
        trans.commit()
        # insert changed rows
    except BaseException:
        trans.rollback()
        raise
    conn.close()


def copy_from(df, tablename):
    """
    pandas dataframe store to a db table using copy_from
    recommand only use for large dataframe
    :param db:sqlalchemy.create_engine(conn_string)
    :param df:pandas.dataframe
    :param tablename:string
    :param columns:
    :return:
    """
    import psycopg2

    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(conn_string(2))
    except (Exception, psycopg2.DatabaseError) as error:
        return error
    cursor = conn.cursor()
    csv_path = './temp.csv'
    df.to_csv(csv_path, index=False, header=False)  # 1.58
    file = open(csv_path, 'r')
    try:
        cursor.copy_from(file, tablename, sep=',')
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        conn.rollback()
        return error
    finally:
        cursor.close()
        file.close()
        # os.remove(csv_path)


def del_by_symbol(symbols, tablename):
    """
    delete exist row, condition is the symbols.
    :param symbols: a pandas.dataframe of symbols
    :param tablename: string
    :return:
    """
    from sqlalchemy import create_engine

    db = create_engine(conn_string())
    conn = db.connect()

    # DB insert temp table
    symbols.to_sql(
        'symbols_tmp',
        con=conn,
        if_exists='replace',
        index=False, method='multi')
    trans = conn.begin()

    db = create_engine(conn_string())
    conn = db.connect()
    trans = conn.begin()

    try:
        # delete those rows that we are going to "upsert"
        db.execute(
            'delete from ' +
            tablename +
            ' where symbol in (select symbol from symbols_tmp)'
        )
        trans.commit()
        # insert changed rows
    except BaseException:
        trans.rollback()
        raise
    conn.close()


def most_recent(symbol, tablename):
    """
    get most recent date from existing rows
    :param symbol: string
    :param tablename: string
    :return max_date as timestamps, return 1 if no records:
    """
    from sqlalchemy import create_engine
    from datetime import datetime

    db = create_engine(conn_string())
    conn = db.connect()
    trans = conn.begin()

    try:
        date = db.execute('select max(date) from ' + tablename +
                          " where symbol='"
                          + symbol
                          + "'"
                          ).first()[0]
        trans.commit()
        if date is not None:
            max_date = int(datetime.timestamp(date))
        else:
            max_date = 0
    except BaseException:
        trans.rollback()
        raise
    conn.close()
    return max_date
