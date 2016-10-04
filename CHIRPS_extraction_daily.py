'''
Created on Sep 30, 2016

@author: timothyjohnson

Script for extracting the values of daily CHIRPS rainfall data from 2000-present.
Adds a time column to the output time based on the date already contained in 
the raster file names of the CHIRPS rasters. Since the output table is so large,
about 45 million rows, to add a date column, the data needs to be read in as 
chunks and output to table as chunks as well. 
'''
# Import system modules
from __future__ import division
import arcpy
import os, time, csv
import pandas as pd
from arcpy import env
from arcpy.sa import *
from dbfpy import dbf

start_time = time.time() # start time to keep track of time 

arcpy.env.overwriteOutput = True # Overwrite pre-existing files

# Check out the ArcGIS Spatial Analyst extension license
arcpy.CheckOutExtension("Spatial")
# Check out the ArcGIS Geostatistical Analyst extension license
arcpy.CheckOutExtension("GeoStats")

#Set paths to your own data file locations. 1st path is CHIRPS rasters, 2nd is
#for the extract data to table output, 3rd is where the point shapefile is 
#located.
pathlist = [r"F:\TimData\Liang\Tanz_weather\CHIRPS\Unzipped" + "\\",
            r"F:\TimData\Liang\Tanz_weather\CHIRPS\extract" + "\\",
            r"F:\TimData\Liang\Tanz_weather" + "\\"]


def makefiles():
    '''create folders if they do not already exist
    '''
    for files in pathlist:
        if not os.path.exists(files): 
            os.makedirs(files)
            print "made " + files  


def extractValues():
    '''Extract values of daily rainfall data to csv table based on household 
       point shapefile. Add option to output table with names of input rasters
    '''
    env.workspace = pathlist[0] # Set workplace for CHIRPS daily rasters
    raslist = arcpy.ListRasters() # create raster list of all rasters
    outname = "CHIRPS_tanz.dbf"
    arcpy.ExtractValuesToTable_ga(pathlist[2]+"BalancedGeoVars.shp", raslist, 
                                  pathlist[1]+outname, 
                                  pathlist[1]+"rasnames.dbf", "FALSE")
    print "Completed extracting values on", outname


def DBFtoCSV():
    '''Convert DBFs to CSVs. 
    '''
    env.workspace = pathlist[1] # Set new workplace where tables are loc.
    tablelist = arcpy.ListTables() # list tables in file
    for table in tablelist: # iterate through every table
        #make sure you are just working with .dbf tables 
        if table.endswith('.dbf'):
            #name csv the same as the .dbf table just with .csv at the end
            csv_fn = table[:-4]+ ".csv"
            with open(pathlist[1]+csv_fn,'wb') as csvfile: #name out path
                in_db = dbf.Dbf(pathlist[1]+table)
                out_csv = csv.writer(csvfile)
                #copy row names and items in rows from dbf to csv
                names = []
                for field in in_db.header.fields:
                    names.append(field.name)
                out_csv.writerow(names)
                for rec in in_db:
                    out_csv.writerow(rec.fieldData)
                in_db.close()
        #keep track of processing
        print "Completed", table[:-4]+".csv table."


def addDateColumn():
    """Adds time to the daily rainfall data. Reads the csv as chunks of 100k 
       rows at a time and outputs them, appending as needed, to a single csv. 
       Uses the column of the raster names to get the date, this was output 
       from the extract values to table tool
    """
    df = pd.read_csv(pathlist[1]+"CHIRPS_tanz.csv", iterator=True, 
                     chunksize=100000) #read csv file as 100k chunks
    newyears = [] #create new empty column to append times to
    rasnames = pd.read_csv(pathlist[1]+"rasnames.csv") #read rasnames file
    rasnamescol = rasnames[rasnames.columns[0]] #column to base raster names
    for rasname in rasnamescol: #iterate through rasnames after column head
        time = rasname[58:-4] #subset full file name of raster to just the time
        print time #check to make sure just time is selected
        newyears.append(time) #create a list of the times used
    newtime = [] #empty list to append repeating times for different rows
    count = 1 #for indexing item in time list 
    chunknum = 0 #keep track of chunk numbers 
    for chunk in df: #for each 100k rows
        #time index needs slight adjustment to make sure all times considered
        count-=1 
        newtime = [] #empty list to append repeating times for different rows
        toiterate = chunk[chunk.columns[2]] #ID of raster nums to base time
        while count <= toiterate.max():
            for i in toiterate: 
                if i ==count:
                    newtime.append(newyears[count])
            count+=1
            print count 
        print "Finished", str(chunknum), "chunks"
        chunk["time"] = newtime #create new column in dataframe based on time
        outname = "CHIRPS_tanz_time2.csv"
        #append each output to same csv, using no header
        chunk.to_csv(pathlist[2]+outname, mode='a', header=None, index=None)
        chunknum+=1
        
        
def main():
    makefiles()
    extractValues()
    DBFtoCSV()
    addDateColumn()

   
if __name__ =="__main__":
    main()
    
print time.time() - start_time, "seconds, finished" # Print out time of finish 

