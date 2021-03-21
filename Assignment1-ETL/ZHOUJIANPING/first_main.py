#first time run, creat tables
import finnhub
import pandas as pd
import psycopg2
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
Table_ND=table[0]['Symbol']
Table_NM=table[0]['Symbol']

#connect to database
engine=connect_to_db
cur=engine.cursor()
enddate=int(datetime.timestamp(time))
for i in range(100):
    engine=connect_to_db()
    cur=engine.cursor()
    Table_ND[i]=DBtable.creat_dailysqltable(cur,engine,SP500_NAME[i])
    df_D=Rdate.df_finhub(SP500_NAME[i],'D',enddate-31622400,enddate)
    time.sleep(1)
    uploaddata_daily(cur,Table_ND[i][i],df_D)
    
for i in range(100):
    engine=connect_to_db()
    cur=engine.cursor()
    Table_NM[i]=DBtable.creat_dailysqltable(cur,engine,SP500_NAME[i])
    df_D=Rdate.df_finhub(SP500_NAME[i],1,enddate-1622400,enddate)
    time.sleep(1)
    uploaddata_daily(cur,Table_NM[i][i],df_D)
    
Table_ND.to_csv('tablenameD.csv')
Table_NM.to_csv('tablenameM.csv')
