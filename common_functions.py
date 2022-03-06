from pyspark.sql import DataFrame
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import os

def cleandata(df: DataFrame) -> DataFrame:
    TimestampColumns = []
    StringColumns = []
    IntegerColumns = []
    for col in df.dtypes:
        if col[1]=='timestamp':
            TimestampColumns.append(col[0])
        if col[1]=='string':
            StringColumns.append(col[0])
        if col[1]=='bigint' or col[1]=='int' or col[1]=='long' or col[1]=='double' or col[1]=='float':
            IntegerColumns.append(col[0])
    df = df.fillna('NotAvailable', subset=StringColumns)
    df = df.na.fill(-1, subset=IntegerColumns)
    for dateCol in TimestampColumns:
        df=df.withColumn(dateCol,to_date(dateCol))
    return df

def replaceNullDatesWithMaximumDate(df: DataFrame, col: str) -> DataFrame:
    max_date=df.agg({col: 'max'})
    max_value=max_date.first()['max({})'.format(col)]
    df=df.withColumn(col,F.when(F.col(col).isNull(),max_value).otherwise(F.col(col)))
    return df

def filterWithMaximumDate(df : DataFrame, col : str) ->DataFrame:
    max_date=df.agg({col: 'max'})
    max_value=max_date.first()['max({})'.format(col)]
    df=df.filter("{}=='{}'".format(col,max_value))
    return df

def file_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
    else:
        raise
        
        
def writeCsv(df :DataFrame,adlsPath:str,writeMode=""):
    writeMode= "overwrite" if writeMode =="" else writeMode
    pathAndFile=os.path.split(adlsPath)
    TEMPORARY_TARGET=pathAndFile[0]+"/{}Temp".format(pathAndFile[1].split('.')[0])
    df.repartition(1).write.mode(writeMode).option("header", "true").option("delimiter",",").csv(TEMPORARY_TARGET)
    temporary_file = os.path.join(TEMPORARY_TARGET, dbutils.fs.ls(TEMPORARY_TARGET)[3][1])
    print(f"adlsPath : {adlsPath}")
    print(f"TEMPORARY_TARGET : {TEMPORARY_TARGET}")
    print(f"temporary_file : {temporary_file}")
    dbutils.fs.cp(temporary_file, adlsPath)
    dbutils.fs.rm(TEMPORARY_TARGET,True)


def writeText(df :DataFrame,adlsPath:str,writeMode=""):
    writeMode= "overwrite" if writeMode =="" else writeMode
    pathAndFile=os.path.split(adlsPath)
    TEMPORARY_TARGET=pathAndFile[0]+"/{}Temp".format(pathAndFile[1].split('.')[0])
    df.repartition(1).write.mode(writeMode).text(TEMPORARY_TARGET)
    temporary_file = os.path.join(TEMPORARY_TARGET, dbutils.fs.ls(TEMPORARY_TARGET)[3][1])
    dbutils.fs.cp(temporary_file, adlsPath)
    dbutils.fs.rm(TEMPORARY_TARGET,True)

def writeFile(df :DataFrame,adlsPath:str,fileFormat:str,writeMode=""):
    writeMode= "overwrite" if writeMode =="" else writeMode
    pathAndFile=os.path.split(adlsPath)
    temp_target=pathAndFile[0]+"/{}Temp".format(pathAndFile[1].split('.')[0])
    df.repartition(1).write.format(fileFormat).mode(writeMode).save(temp_target)
    temporary_file = os.path.join(temp_target, dbutils.fs.ls(temp_target)[3][1])
    dbutils.fs.cp(temporary_file, adlsPath)
    dbutils.fs.rm(temp_target,True)
