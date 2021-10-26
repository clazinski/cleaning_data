#%%imports
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 26 07:54:40 2021

@author: camilai
"""
#imports
import pandas as pd
from sqlalchemy import create_engine, types, text
from datetime import datetime
import os


#dict connection oracle
connection_oracle = {
'table_name': 'table_name',
'database_name': 'database_name',
'url_db': 'oracle+cx_oracle://user:password@host:port/?service_name='
}

#%%extract data

#path to file
path = 'path:\\to\\file.txt'

#reading file
data = pd.read_fwf(path, colspecs='infer')

data = pd.DataFrame(data)

#extracting last modified date from file
epoch_time = os.path.getmtime(path)
#transforming epoch time
modification_date = datetime.fromtimestamp(epoch_time)




#%%clean data

#adding and renaming columns
data.columns = ['Column_1', 'Column_2']
data.insert(2, 'Column_3', None)
data.insert(3, 'Column_4', None)
data.insert(4, 'Column_Date', modification_date)

#excluding the first lines
data = data.drop([0, 1, 2, 3])

#excluding null lines
data = data[data.Country.notnull()]

#splitting columns
data[['Column_2','Column_3']] = data['Column_2'].str.split(',', 1, expand=True)
data[['Column_3','Column_4']] = data['Column_3'].str.split(',', 1, expand=True)

#changing uppercase to lowercase
data['Column_2'] = data['Column_2'].str.lower()

#taking off the white spaces
data['Column_1'].str.strip()
data['Column_3'].str.strip()
data['Column_4'].str.strip()

#%%connection to oracle

url_oracle = connection_oracle['url_db'] + connection_oracle['database_name']
conn_oracle = create_engine(url_oracle)

#sql
sql = '''SELECT MAX ("Column_Date") AS "Max" FROM table_name'''
result = conn_oracle.execute(text(sql))

#extracting query result into a list
dt_compare = result.scalars().all()

#passing list to datetime to compare
dt_compare = pd.to_datetime(dt_compare)

result.close()

#%%send to oracle

#if the last file modification date > the highest date pulled from the database, append
if (modification_date > dt_compare):
    #sending to oracle, passing the types of each column
    data.to_sql(connection_oracle['table_name'], conn_oracle, if_exists='append', index=False, dtype=
                {'Column_1' : types.String(100),
                 'Column_2' : types.String(100),
                 'Column_3' : types.String(50),
                 'Column_4' : types.String(100),
                 'Column_Date' : types.TIMESTAMP()
                 })
        






