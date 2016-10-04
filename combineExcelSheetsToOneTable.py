'''
Created on Sep 14, 2015

@author: TIMOTHYJOHNSON

This script reads an excel file with 17 sheets, each containing 5 columns all in the same place in each sheet. 
For example, column one is household ID in each sheet, column 2 is latitude midline. Each sheet represents a 
different states data. The script creates one table with all the sheets, adding a state column for each sheet
with the states name in the row. 
'''

from __future__ import division
import time
import os
import pandas as pd
import six

#start time
start_time = time.time()

#set path to folder containing excel file, insert your folder location. 
path = [r"F:\TimData\FadamaIII" + "\\"] 

#create an empty dataframe using pandas, where each new sheet info will be appended into.   
alldata = pd.DataFrame()
      
def extractcolumns():
    """For each sheet of data, this function adds a state column containing the name of the state.
       (The name of the state is the sheets name.) It then appends the dataframe into a list to 
       be joined into one dataframe containing all of the sheets data. 
    """
    #set sheet number at 0 to represent first state sheet
    sheetnum = 0 
    #Read excel file using pandas, specifying the path of folder 
    xl = pd.ExcelFile(path[0] + "Midline_Endline_GPS_SentToTim.xlsx")
    #get the sheet names stored as sheets variable to be used to fill in the states column 
    sheets = xl.sheet_names
    #create empty list to store dataframe outputs  
    totaldflist = []
    #for each sheet in excel sheet do the following
    #While the sheet number is less than the total number of sheets
    while sheetnum < len(sheets):
        #parse each sheet to df. The first row is the header which is the default
        df = xl.parse(sheets[sheetnum])
        #drop rows with no data if they exist 
        nadf = df.dropna()
        #get HH id column to iterate through, adding state row to state column
        HHcol = nadf[df.columns[0]]
        #create empty list for states to be put into 
        statelist = []
        #get the name of state to add to column
        state = sheets[sheetnum]
        #add the state name for each row of LGA
        for i in HHcol:
            #append state name to empty list
            statelist.append(state)
        #doublecheck results 
        print "/nHere is the state list: ", str(statelist) + "\n" 
        #create new column named state, containing the state name for each row    
        nadf["state"] = statelist
        #print dataframe to double check results 
        print nadf 
        print "\nFinished creating output for: " + str(sheets[sheetnum]) + "\n"
        #now append results to total df list each iteration 
        totaldflist.append(nadf)
        #add one to sheetnum to move to next sheet 
        sheetnum = sheetnum + 1  
    #concatenate dataframe list to one dataframe  
    finalresult = pd.concat(totaldflist)
    print "\nHere is the final result\n" 
    #print dataframe list to view results 
    print finalresult
    #save output to csv
    finalresult.to_csv(path[0] + 'MidlineEndlineGPS_cleaned.csv')  

def main():
    """main function to implement extract column function
    """
    extractcolumns()

    
if __name__ == "__main__":
    main()


#print time script finished  
print time.time() - start_time, "seconds, finished"    
