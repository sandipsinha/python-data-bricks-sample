import gnupg
from pprint import pprint
import time
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import os.path
import pandas as pd


try:
    gpg = gnupg.GPG('/usr/local/Cellar/gnupg/2.2.5/bin/gpg',gnupghome='/Users/ssinha/python-data-bricks-sample/gpgkeys')
    key_data = open('/Users/ssinha/python-data-bricks-sample/gpgkeys/slim.shady.sec.asc').read()
    gpg.import_keys(key_data)
    fname = 'titanic-decrypted.csv'
    with open('titanic.csv.gpg', 'rb') as f:
        gpg.decrypt_file(f, output=fname)
        
    if os.path.isfile(fname):
        #Create the config for the SparkContext
        conf = SparkConf().setMaster("local").setAppName("TitanicDS").set("spark.executor.memory", "1g")
        #Create the Context
        sc = SparkContext(conf = conf)
        #Read the file using Pandas 
        pandas_df = pd.read_csv("titanic-decrypted.csv")
        #Initilize the na values with 0
        pandas_df.fillna(0,inplace=True)
        #Create a SqlContext
        sqlCtx = SQLContext(sc)
        #Create a spark Dataframe (take advantage of the distributed processing)
        spark_df=sqlCtx.createDataFrame(pandas_df)
        #Create a in memory table for column calculations
        spark_df.createOrReplaceTempView("titanic")
        #query for calculating the average age of the passengers
        sqlDF = sqlCtx.sql("SELECT avg(Age) meanage FROM titanic where Age > 0")
        print("The mean age is {}".format(sqlDF.collect()[0].meanage))
        #query for calculating the 75th percentile age of the passengers
        sqlDF = sqlCtx.sql("SELECT Percentile(Age,.75) qtrpercent FROM titanic where Age > 0")
        print("The 75 percentile age is {}".format(sqlDF.collect()[0].qtrpercent))
        #Finally Write it out to a Parquet File
        spark_df.select("*").write.save("titanicDF.parquet")
except ValueError:
    print 'The error was {}'.format('a value error')
except Exception as e:
    print 'The other error was {}'.format(e)

    

 