def creat_dailysqltable(cur,engine,Sysblem):
    name_TableD="DailyD"+Sysblem
    sqlCreateTable ="create table "+name_TableD+" (close_price FLOAT, high_price FLOAT, low_price FLOAT, open_price FLOAT,s varchar(255),t INT,volume INT);"
    cur.execute(sqlCreateTable)
    engine.commit()
    return name_TableD

def creat_onemsqltable(cur,engine,Sysblem):
    name_TableM="DailyM"+Sysblem
    sqlCreateTable ="create table "+name_TableM+" (close_price FLOAT, high_price FLOAT, low_price FLOAT, open_price FLOAT,s varchar(255),t INT,volume INT);"
    cur.execute(sqlCreateTable)
    engine.commit()
    return name_TableM


def uploaddata_daily(cur,name_TableD,df_D):
    cols = ",".join([str(i) for i in df_D.columns.tolist()])
    table_name=name_TableD
    # Insert DataFrame recrds one by one.
    for i,row in df_D.iterrows():
        sql = "INSERT INTO " +name_TableD+" (" +cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
        cur.execute(sql, tuple(row))
