'''
Created on Mar 9, 2016

@author: TIMOTHYJOHNSON

This scripts calculates the area of each polygon for ~12000 shapefiles, 
adding an area column, then the tables from each shapefile are converted to 
csv files, then stacked using pandas 
'''
# Import system modules
from __future__ import division
import arcpy
from arcpy import env
from arcpy.sa import *
import os, sys, csv, time
import pandas as pd
import glob 

pathlist = [r"F:\TimData\CGIAR_CRP\Biodiversity_DB" + "\\", 
            r"F:\TimData\CGIAR_CRP\Biodiversity_DB\IndividualShapefiles" 
            r"\dissolve" + "\\",
            r"F:\TimData\CGIAR_CRP\Biodiversity_DB\IndividualShapefiles" 
            r"\csvs" + "\\"] 

start_time = time.time()

arcpy.env.overwriteOutput = True

arcpy.CheckOutExtension("Spatial")


def makefiles():
    '''create folders if they do not already exist
    '''
    for files in pathlist:
        if not os.path.exists(files): 
            os.makedirs(files)
            print "made " + files  


def addArea():
    '''add area fields to all dissolved shapefiles'''
    env.workspace = pathlist[1] 
    polylist = arcpy.ListFeatureClasses()
    for poly in polylist:
        try:
            arcpy.AddGeometryAttributes_management(poly, "AREA_GEODESIC", "#", 
                                                   "SQUARE_KILOMETERS")
            print "Finished adding area for:", poly
        except Exception:
            print "Encountered error with", poly 
            print arcpy.GetMessages()
            pass 


def exportToCsvs():
    '''exports every shapefile to csv'''
    env.workspace = pathlist[1] 
    polylist = arcpy.ListFeatureClasses()
    for poly in polylist:
        try:
            outname = str(poly)[:-9]+".csv"
            print outname
            arcpy.ExportXYv_stats(poly,["SCINAME","AREA_GEO"], "COMMA",
                                  pathlist[2]+outname, "ADD_FIELD_NAMES")

            print "Finished exporting:", outname
        except Exception:
            print "Encountered error with", outname 
            print arcpy.GetMessages()
            pass


def stackdata():
    alltxts = glob.glob(pathlist[2] + "/*.csv") # grab all csv files in list
    firsttxt =  alltxts[:1] # text to append the others to
    totaldf = pd.read_csv(firsttxt[0])
    testtxts = alltxts[1:] # list of every csv except the first
    for txt in testtxts:
        df = pd.read_csv(txt)
        totaldf = totaldf.append(df) # append every csv to one another in list 
        print "Finished", txt
    return totaldf   
        
        
def main():
    makefiles()
    addArea()
    exportToCsvs()
    stackedcsv = stackdata()
    stackedcsv.to_csv(pathlist[0] + 'TotalStackedBirdData_Areas.csv') 
    
    
if __name__ == "__main__":
    main()

#Print out time of finish and verify success of finish
print (time.time() - start_time) / 60, "minutes, finished"