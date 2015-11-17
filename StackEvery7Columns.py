'''
Created on Nov 17, 2015

@author: timothyjohnson

Stack every 6 columns of data of a long horizontal table based on common columns.
Useful when multiple entries have been entered in survey data of similar data. 
'''
import pandas as pd 
import time 

#start time
start_time = time.time()

#set path to folder containing excel file, insert your folder location. 
path = [r"F:\TimData\UpperPradesh" + "\\"] 

def stackcolumns():
    #load csv 
    try:
        df = pd.read_csv(path[0] + "Village_GPS.csv")
    except:
        print "Error with csv file"
        print Exception.message
    #get length of number of columns
    collist = df.columns.values.tolist()
    #variables for starting and ending slice position 
    lastindex = 18 
    firstindex = 11
    #to add to starting and ending slice position 
    addon = 7 
    #set borehole count to add a column containing count of borehole 
    borecount = 1 
    #establish first dataframe to append the others to 
    bore1 = df.iloc[:,firstindex:lastindex]
    #rename columns so they can be more easily matched
    bore1.columns =['latitude', 'longitude', 'altitude', 'accuracy', 
                       'readingFromDistance', 'DistanceToReading', 
                       'ReadingDirection']
    bore1["borehole"]=borecount
    #add 7 to both starting and ending slice index before while loop
    lastindex+=addon
    firstindex+=addon
    while lastindex<len(collist):
        #add one to borecount 
        borecount+=1
        bore = df.iloc[:,firstindex:lastindex]
        bore.columns =['latitude', 'longitude', 'altitude', 'accuracy', 
                       'readingFromDistance', 'DistanceToReading', 
                       'ReadingDirection']
        bore["borehole"]=borecount
        bore1 = bore1.append(bore)
        lastindex+=addon
        firstindex+=addon
        print bore
    print "\nHere is bore1: \n", bore1  
    return bore1 

    
    
def main():
    newcsv = stackcolumns()
    newcsv.to_csv(path[0] + 'Village_GPS_stackedData.csv') 
    
if __name__ =="__main__":
    main()


print time.time() - start_time, "seconds, finished"    
    