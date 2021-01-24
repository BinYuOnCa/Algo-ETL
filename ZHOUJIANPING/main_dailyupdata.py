#Daily update creat tables
import finnhub
import pandas as pd
import psycopg2
import time
import sqlalchemy
import numpy as np
import os 
import os.path
import connect_db
import Rdate
import DBtable
from datetime import datetime
from dotenv import load_dirname


#download the SP500 stocks as taget 
table=pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
SP500_NAME=table[0]['Symbol']
Table_ND=pd.read_csv('tablenameD.csv')
Table_NM=pd.read_csv('tablenameM.csv')

#connect to database
engine=connect_to_db
cur=engine.cursor()
enddate=int(datetime.timestamp(time))
cur.execute("SELECT MAX(timestamp) FROM DailyD;")
lasttime=cur.fetchall()[0][0]
Starttime=lasttime+1
for i in range(100):
    engine=connect_to_db()
    cur=engine.cursor()
    Table_ND[i]=DBtable.creat_dailysqltable(cur,engine,SP500_NAME[i])
    df_D=Rdate.df_finhub(SP500_NAME[i],'D',Starttime,enddate)
    time.sleep(1)
    Tabledf=Table_ND[i:i+1]
    Table_nameD=np.array(Tabledf)[0,1]
    uploaddata_daily(cur,Table_nameD,df_D)
    
for i in range(100):
    engine=connect_to_db()
    cur=engine.cursor()
    Table_NM[i]=DBtable.creat_dailysqltable(cur,engine,SP500_NAME[i])
    df_D=Rdate.df_finhub(SP500_NAME[i],1,Starttime,enddate)
    time.sleep(1)
    Tabledf=Table_NM[i:i+1]
    Table_nameM=np.array(Tabledf)[0,1]
    uploaddata_daily(cur,Table_nameM,df_D)
