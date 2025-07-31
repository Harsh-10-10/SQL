import pandas as pd
import sqlite3
df=pd.read_csv("eg.csv")
conn=sqlite3.connect('example.db')
df.to_sql('Details',conn,if_exists='replace',index=False)
conn.close()

