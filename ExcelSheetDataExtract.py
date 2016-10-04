'''
Created on Sep 10, 2015

@author: timothyjohnson

I received an excel file with 38 sheets, 37 of them representing data from a different state in Nigeria 
(the extra one is a summary sheet). There are two columns of interest in each sheet, column A 
and column B, which represent state and LGA (local government area) names. This script: 1) extracts 
the LGA column from each sheet, 2) removes empty rows in the LGA column, and 3) removes duplicates, 
it also 4) populates a new column representing the states name, and 5) joins all of the data into one excel
file containing two columns: states and LGA. 
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
    """This function takes each sheet of data and extracts the LGA column. Then, any duplicate rows 
       are removed, then no data values are removed. Next, for each row in the lga column, add a 
       row containing the state name in a created 'state' column. Finally join all dataframes for 
       each sheet together into one, concatenate, and output to excel file. 
    """
    #set sheet number at 1 to represent first state sheet (sheet 0 is a summary sheet)
    sheetnum = 1 
    #Read excel file using pandas, specifying the path of folder 
    xl = pd.ExcelFile(path[0] + "LIST_OF_FCA_FUG_And_SUBPROJECTS.xls")
    #get the sheet names stored as sheets variable
    sheets = xl.sheet_names
    #create empty list to store output  
    totaldflist = []
    #for each sheet in excel sheet do the following
    #While the sheet number is less than the total number of sheets
    while sheetnum < len(sheets):
        #parse lga column to df. Specify column of interested as the "B" column. 
        #"B" is used instead of the column name because it is different in each sheet
        #Set column header to "None" because the columns name starts at a diff line in diff sheets
        df = xl.parse(sheets[sheetnum], None, None, 0, None, "B")
        #drop duplicate values 
        dupdf = df.drop_duplicates()
        #drop rows with no data 
        nadf = dupdf.dropna()
        #drop first value of df since it is the column header
        slicedf = nadf.ix[1:]
        #get lga column to iterate through, adding state row to state column
        lgacol = slicedf[df.columns[0]]
        #create empty list for states to be put into 
        statelist = []
        #create empty list for lowercase lga 
        lowerlgalist = []
        #create empty list for lowercase states 
        lowerstatelist = []
        #get the name of state to add to column
        state = sheets[sheetnum]
        #add the state name for each row of LGA, also make lga rows lowercase
        for i in lgacol:
            #append state name to empty list
            statelist.append(state)
            #encode to unicode capable encoding, otherwise error will occur with symbols like " or ,
            #will need to remove these later 
            y = i.encode("utf-8")
            #convert lga to string 
            lgastr = str(y)
            #make each lga lowercase
            lowerlga = str.lower(lgastr)
            #append result to lower case lga list 
            lowerlgalist.append(lowerlga)
        #now make each state name lowercase    
        for i in statelist:
            #convert statename to string 
            statestr = str(i)
            #make each state lowercase
            lowerstate = str.lower(statestr)
            #append result to lower case state list 
            lowerstatelist.append(lowerstate)
        #doublecheck results 
        print "/nHere is the lower lga list: ", str(lowerlgalist) + "\n"
        print "/nHere is the lower state list: ", str(lowerstatelist) + "\n" 
        #create new column named lga, containing the lga name for each row    
        slicedf["lga"] = lowerlgalist 
        #create new column named state, containing the state name for each row    
        slicedf["state"] = lowerstatelist
        #print dataframe to double check results 
        print slicedf 
        print "\nFinished creating output for: " + str(sheets[sheetnum]) + "\n"
        #delete the first column of capitalized lga names
        del slicedf[df.columns[0]] 
        print "deleted first column: ", str(slicedf) 
        #now append results to total df list each iteration 
        totaldflist.append(slicedf)
        #add one to sheetnum to move to next sheet 
        sheetnum = sheetnum + 1  
    #concatenate dataframe list to one dataframe  
    finalresult = pd.concat(totaldflist)
    print "\nHere is the final result\n" 
    #print dataframe list to view results 
    print finalresult
    #save output to csv
    finalresult.to_csv(path[0] + 'FadamaIIIcleanedLGAs7.csv') 
    #optional save output to excel, doesn't work as is due to ascii error 
    #finalresult.to_excel(path[0] + 'FadamaIIIcleanedLGAs7.xls') 

  
def main():
    """main function to implement extract column function
    """
    extractcolumns()

    
if __name__ == "__main__":
    main()


#print time script finished  
print time.time() - start_time, "seconds, finished"    
    