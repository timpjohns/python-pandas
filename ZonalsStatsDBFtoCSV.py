'''
Created on Feb 2, 2016

@author: TIMOTHYJOHNSON

Iterates through CRU climate data and gets zonal stats for specified admin area. 
Converts output dbf files into csv files and then appends them into one csv.

Requirements: ArcGIS license and spatial analyst extension, pandas, glob, and 
dbfpy-needed to convert dbf files to csv.  
'''
# Import system modules
from __future__ import division
import arcpy
from arcpy import env
from arcpy.sa import *
import os, sys, csv, time
import pandas as pd
import glob 
from dbfpy import dbf

# start time to keep track of time 
start_time = time.time()

# Overwrite pre-existing files
arcpy.env.overwriteOutput = True

# Check out the ArcGIS Spatial Analyst extension license
arcpy.CheckOutExtension("Spatial")

# specify full path location of rasters and admin shapefile first, 
# then specify output of zonal stats folder location, 
# next specify output of csvs,
# finally list final table location of table
pathlist = [r"F:\TimData\DataTests\CRUZonalStats\pet" + "\\", 
            r"F:\TimData\DataTests\CRUZonalStats\pet_zonalStats" + "\\",
            r"F:\TimData\DataTests\CRUZonalStats\pet_csv" + "\\",
            r"F:\TimData\DataTests\CRUZonalStats\pet_finalTable" + "\\"]


def zonalstats(poly, polycolumn, stat):
    '''Run zonal stats on every raster in folder for the polygon, polygon 
       column, and stats specified
    '''
    # Set workplace for rasters and admin polygon  
    env.workspace = pathlist[0]
    # create raster list of all rasters 
    raslist = arcpy.ListRasters()
    # set count for adding to the end of zonal stats table name  
    count = 1
    #iterate through rasters 
    for ras in raslist: 
        #define output name
        name = "ZonalStats" + str(count) + ".dbf"
        outZSat = ZonalStatisticsAsTable(poly, polycolumn, ras, 
                                         pathlist[1]+name,"DATA", stat)
        #keep track of processing
        print "\n Processing ",name," complete."
        count = count + 1


def DBFtoCSV():
    '''Convert every DBF table into CSV table. 
    '''
    # Set new workplace where tables are located 
    env.workspace = pathlist[1]
    # list tables in file
    tablelist = arcpy.ListTables()   
    # iterate through every table
    for table in tablelist:
        #make sure you are just working with .dbf tables 
        if table.endswith('.dbf'):
            #name csv the same as the .dbf table just with .csv at the end
            csv_fn = table[:-4]+ ".csv"
            with open(pathlist[2]+csv_fn,'wb') as csvfile:
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
        print "\n Processing ",table[:-4]+".csv table complete."


def MergeTables(outtable):
    '''Merges every csv file into one csv, with specified output name
    '''
    allFiles = glob.glob(pathlist[2] + "/*.csv")
    tablelist = []
    for f in allFiles:
        df = pd.read_csv(f, index_col=None, header =0)
        df.drop(["COUNT", "AREA", "ZONE_CODE"], inplace=True, axis=1)
        tablelist.append(df)
    totaldata = pd.concat(tablelist, axis=1, join_axes=[tablelist[0].index])
    totaldata.to_csv(pathlist[3] + outtable)     
    

def main():
    '''Run zonal stats, dbf to csv function, and merge tables function
    '''
    #name admin poly 
    adminpoly = "g2015_2014_0_minus_antarctica.shp"
    #name admin polygon column of interest (ex: state, county, country, etc..)
    adminpolycolumn = "ADM0_NAME"
    #name statistic of interest in all caps 
    zonalstat = "MEAN"
    #name final output name
    finaltable = "PET_zonalStats.csv"
    
    zonalstats(adminpoly, adminpolycolumn, zonalstat)
    DBFtoCSV()  
    MergeTables(finaltable)  
    
    
if __name__ =="__main__":
    main()
    
    
#Print out time of finish
print time.time() - start_time, "seconds, finished"    
