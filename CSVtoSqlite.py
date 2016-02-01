'''
Created on Feb 1, 2016

@author: timothyjohnson

Converts large table with ~2.6 million rows and 70 variables to sqlite table 
using pandas. 

Errors and Issues of Note:
For some reason python.exe shuts down after importing about 700k rows into 
sqlite database. I made the script where the csv is broken up into four sections
and 4 different sqlite tables are created, which are then merged within sql. 
'''

import pandas as pd 
import sqlite3
import time
import numpy as np

#start time
start_time = time.time()
#set path to folder containing excel file, insert your folder location. 
path = [r"F:\TimData\SQLiteTest" + "\\"]


def getColumnNames(headercsv):
    '''get column names by reading first row of data''' 
    df = pd.read_csv(path[0] + headercsv, nrows = 1)   
    return df.columns.values
     
     
def defineDataTypes(columns):
    '''define the data types of each column in a dict to pass to csv read'''
    dtypedict = {}
    #dict of column data types, iterate through column list above
    colcount = 0  
    datatype = None
    for column in columns:
        if colcount in (55,56,58,59,61,62,64):
            datatype = np.dtype('a64') # 64-character string 
        else:
            datatype = np.int32 # 32-bit integer
        dtypedict[columns[colcount]]=datatype
        colcount+=1 
    return dtypedict


def getNumberOfLinesCSV(noheaderCSV):
    '''Get number of lines in the CSV file without headers, used to iterate 
       through
    '''
    nlines = 0
    for line in open(path[0] + noheaderCSV):
        nlines+=1
    return nlines 


def CSVtoSQLite(nlines,noheaderCSV,dtypedict,columns,sqldatabase, sqlouttable):
    '''Using pandas to convert csv with no headers to specified SQLite database
       table. Using dtypedict to set the datatypes of the columns and columns 
       to set the columns needed to append to each iteration. Nlines is used to 
       iterate through the total # of lines
    '''
    # connect to database
    cnx = sqlite3.connect(path[0]+sqldatabase)
    cnx.text_factory = str 
    # Iteratively read CSV and dump lines into the SQLite table
    itercount = 1 
    #data has to be broken up into chunks to parse from pandas df to sqlite 
    #table.Set at 100k rows but can be manipulated with.
    for i in range(0, nlines, 100000):
        df = pd.read_csv(path[0] + noheaderCSV, 
                dtype = dtypedict, #dict of column data types          
                header=None,  # no header, define column header manually later
                low_memory=False, # otherwise will receive data type warning due 
                # to mixed types 
                nrows=100000, # number of rows to read at each iteration
                skiprows=i)   # skip rows that were already read
        # columns to read        
        df.columns = columns
        df.to_sql(name=sqlouttable, con=cnx, 
                    index=False, # don't use CSV file index
                    index_label='TableID', # use a unique column from DataFrame 
                    # as index
                    if_exists='append') 
        itercount+=1
    cnx.close()   


def main():
    #had to create two csv's, one with header columns and one without, so I 
    #can use the one with headers to get a list of the column names. Could 
    #be done more cleanly most likely. 
    csv_headers = 'FullMODISdata_700k_headers.csv'
    csv_noheaders = 'FullMODISdata_700k_noheaders.csv'
    sqldatabase = "MODISSQL_700k.sqlite"
    sqlouttable = "MODIS_data_first700k"
    
    columns = getColumnNames(csv_headers)
    dtypedict = defineDataTypes(columns)
    nlines = getNumberOfLinesCSV(csv_noheaders)
    CSVtoSQLite(nlines,csv_noheaders,dtypedict,columns,sqldatabase, sqlouttable)
    
    
if __name__ == "__main__":
    main()    

print time.time() - start_time, "seconds, finished"   