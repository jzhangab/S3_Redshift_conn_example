# -*- coding: utf-8 -*-
"""
Created on 1/17/2019

@author: zhangj
"""
'''

'''
import pandas as pd
import numpy as np
import datetime as dt
import csv
import os
import psycopg2
import s3fs
import boto3
from io import StringIO
from datetime import datetime

# GLOBALS ###########################################
redshift_table = 'domain.redshift_table'
default_conn = "dbname='db_name' port='1234' user='username' password='password' host='enter_ip_here'"

col_list = [col1, col2,col3
col_string = ''
for i in range(0, len(col_list)):
    if i != len(col_list)-1:
        col_string = col_string + col_list[i].lower() + ', '
    else: 
        col_string = col_string + col_list[i].lower()
        
s3 = 's3_bucket'
directory = 's3_directory'

###################################################

# Function to grab s3 data - can read excel or csv
def get_s3_file(folder, file, t, c):
    print('Reading s3 file: '  + directory + '/' + folder + '/' +file)
    if t == 'list':
        df = pd.read_csv('s3a://' + s3 + '/' + directory + '/' + folder + '/' + file, header=None)
        return(df.values.tolist())
    else:
        if c == 'csv':
            try:
                df = pd.read_csv('s3a://' + s3 + '/' + directory + '/' + folder + '/' + file, encoding = "ISO-8859-1")
            except:
                df = pd.read_csv('s3a://' + s3 + '/' + directory + '/' + folder + '/' + file, encoding = "utf-8")
        else:
            df = pd.read_excel('s3a://' + s3 + '/' + directory + '/' + folder + '/' + file)
        return(df)
        
# Function to save s3 data
def save_s3_file(folder, file, df):
    print('Writing s3 file: ' + directory + '/' + folder + '/' +file)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(s3, directory + '/' + folder + '/' + file).put(Body=csv_buffer.getvalue())
    
# Redshift data retrieval by year within table
def redshift_connection(redshift_table, year):
    conn_string = default_conn    
    conn = psycopg2.connect(conn_string);
    df = pd.read_sql_query('SELECT ' + col_string + ' FROM ' + redshift_table + ' WHERE EXTRACT(YEAR FROM ' + date_open + ') = ' + str(year) + ';', conn)
   
   return(df)
