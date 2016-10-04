'''
Created on Feb 8, 2016

@author: timothyjohnson

Converts a table with 252 columns and 2766 rows to sqlite. Since it would be 
painful to list all of the datatypes and names of the different columns, 
messytables is used to, in this case accurately, guess what they are. 
Then uses pandas to receive headers and datatypes in order to convert to 
sqlite table. 
'''
import pandas as pd 
import sqlite3
import time
import numpy as np
import messytables

#start time
start_time = time.time()
#set path to folder containing excel file, insert your folder location. 
path = [r"F:\TimData\UpperPradesh\WaterTableDepth" + "\\"]


def headersDataTypes(CSV):  
    '''Get column headers and data types using messytables'''  
    table = open(path[0]+CSV, 'rb')
    # Creates a set of tables as file object, although it'll just be one
    tableset = messytables.CSVTableSet(table) 
    rowset = tableset.tables[0] # get first and only table as iterator
    # guesses header names and offset of header, returns headers as list
    offset, headers = messytables.headers_guess(rowset.sample) 
    print "Here is the offset", str(offset), "\nHere are the headers:\n"\
    , str(headers) # test 
    # establish headers in table
    rowset.register_processor(messytables.headers_processor(headers))
    # begin iterator at content, rather than header
    rowset.register_processor(messytables.offset_processor(offset + 1))
    # guess column types, return as list
    types = messytables.type_guess(rowset.sample, strict=True)
    print "Here are the data types", str(types)  
    dtypedict = {} # empty dictionary to append columns and datatype needed
    # for pandas csv to dataframe conversion
    colcount = 0  # location to append datatypes to match columns in dict
    for column in types:
        dtypedict[headers[colcount]]=column
        colcount+=1
    return headers, dtypedict  


def CSVtoSQLite(CSV,dtypedict,columns,sqldatabase, sqlouttable):
    '''Uses pandas to convert input csv to specified SQLite database and
       table (sqldatabase, sqlouttable). Dtypedict is used to set the datatypes 
       of the columns. Columns are the header names. 
    '''
    # connect to database
    cnx = sqlite3.connect(path[0]+sqldatabase)
    cnx.text_factory = str 
    df = pd.read_csv(path[0] + CSV, dtype = dtypedict, # dict of col data types          
                     header=None, # no header, define col header manually later
                     low_memory=False) # otherwise annoying error may show 
    #drop first row, since it contains headers
    df.drop(df.index[:1], inplace=True)
    df.columns = columns # set columns collected from messytables
    df.to_sql(name=sqlouttable, con=cnx) 
    cnx.close()   


def main():
    CSV = "WaterTableData.csv" # input csv
    sqldatabase = "WaterTable.sqlite" # output sqlite database
    sqlouttable = "WaterTableData" # output sql table 

    columns, dtypedict = headersDataTypes(CSV)
    CSVtoSQLite(CSV,dtypedict,columns,sqldatabase, sqlouttable)

    
if __name__ == "__main__":
    main()    

print time.time() - start_time, "seconds, finished"   