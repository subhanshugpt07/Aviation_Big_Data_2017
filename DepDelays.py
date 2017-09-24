from pyspark.sql.functions import *
import csv
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
from pyspark.sql import Row
import csv
from pyspark.sql import SQLContext

def parseCSV(idx, part):
    if idx==0:
        part.next()
    for p in csv.reader(part):
        yield Row(ORIGIN=p[14],
                  DEP_DEL15 = p[33])

def main(sc):
    spark = HiveContext(sc)
    sqlContext = HiveContext(sc)
    print "holaaaaa"
    rows = sc.textFile('../lmf445/Flight_Project/Data/864625436_T_ONTIME_2*.csv').mapPartitionsWithIndex(parseCSV)
    df = sqlContext.createDataFrame(rows)

    df = df.withColumn('DEP_DEL15', df['DEP_DEL15'].cast('int'))
    df = df.na.drop()

    delay_counts = df.select('ORIGIN', 'DEP_DEL15').groupby('ORIGIN').sum().withColumnRenamed('sum(DEP_DEL15)',
                                                                                              'origin_delay_count')
    delay_counts = delay_counts.join(flight_origin, delay_counts.ORIGIN == flight_origin.Airport_origin)
    delays_origin = delay_counts.select('Airport_origin',
                                        (delay_counts.origin_delay_count / delay_counts.origin_count).alias(
                                            '%_flights_departing_15+_minutes_late'))

    delays_origin = delays_origin.sort(desc('%_flights_departing_15+_minutes_late'))

    delays_origin.toPandas().to_csv('Output/MostDepartureDelays.csv')



if __name__ == "__main__":
    sc = SparkContext()
    main(sc)
# In[ ]: