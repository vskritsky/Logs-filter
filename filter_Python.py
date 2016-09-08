# -*- coding: utf-8 -*-
"""
Created on Tue Sep 06 14:25:48 2016

@author: vskritsk
"""
import pandas as pd
import numpy as np
import os 
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_rows', 280)

###Here it detects recursevly all the .txt files and 
###creates all files structure as ladder
for root, dirs, files in os.walk("Data",topdown=False):
    for name in files:
        path=os.path.join(root, name)
        for file_ in path:
            df = pd.read_table(path,index_col=False, 
                    na_values=[''], header=1, sep='\t', 
                    error_bad_lines=False, lineterminator='\n')
### Here we create a signle dataframe from all the log files which
### were found on localhost
df.columns=['Data']

### Here we slice one big dataframe in multiple columns depending on
### formatting. But, as we can see log formate is different.
crid=df[df['Data'].str.contains('|crid|')==True]
df['Date']=df['Data'].str.extract('(....-..-..)',expand=True)
df['Time']=df['Data'].str.extract('(..:..:......)')
df['Message']=df['Data'].str[40:]
df['Exception']=df['Data'].str.extract('(Exception)')

### I took transaction's ID length equal to 13
df['ID_len_13']=df['Message'].str.extract('(crid=.............)\s')
### ID's of transactions can be of length from 1 to inf+. However, solution
### is not trivial and requires more time.
df['ID_len_12']=df['Message'].str.extract('(crid=............)\s')
df['ID_len_11']=df['Message'].str.extract('(crid=...........)\s')
df['ID_len_10']=df['Message'].str.extract('(crid=..........)\s')
df['ID_len_9']=df['Message'].str.extract('(crid=.........)\s')
df['ID_len_8']=df['Message'].str.extract('(crid=........)\s')
df['ID_len_7']=df['Message'].str.extract('(crid=.......)\s')

df['ID_all']=pd.concat([df['ID_len_12'].dropna(), df['ID_len_11'].dropna(),
                       df['ID_len_10'].dropna(), df['ID_len_9'].dropna(),
                       df['ID_len_8'].dropna(), 
                       df['ID_len_7'].dropna()]).reindex_like(df)


### We create a one dataframe with data which we need.
df['Connect']=df['Message'].str.extract('(->)')
df['Disconnect']=df['Message'].str.extract('(<~)')
df['Status']=pd.concat([df['Connect'].dropna(), df['Disconnect'].dropna(),]).reindex_like(df)

### Creating a final dataframe with transactions which are not close
### And/or having an exception
a=pd.concat([df['Date'],df['Time'],df['ID_all'],df['Status'],df['Exception']], axis=1)
#a=pd.concat([df['ID_len_13'], df['Status']], axis=1)
### Droping NaN values to clean the dataframe from broken log records

a=a.dropna(thresh=2)

### Drop transactions which are duplicated. Remained transactions are those,
### which are not closed
c=a.drop_duplicates(['ID_all'])
print(c)