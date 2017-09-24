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
                  ORIGIN_AIRPORT_ID = str(p[11]),
                  DEST = p[23],
                  DEST_AIRPORT_ID = str(p[20]),
                  ROUTE = (p[11],p[20]))
def main(sc):
    spark = HiveContext(sc)
    sqlContext = HiveContext(sc)
    rows = sc.textFile('../lmf445/Flight_Project/Data/864625436_T_ONTIME_2*.csv').mapPartitionsWithIndex(parseCSV)
    df = sqlContext.createDataFrame(rows)

    df_unique = df.select(df.ROUTE.alias('ROUTE_UNIQUE'), 'DEST', 'ORIGIN').distinct()

    busyest_route_single = df.select('ROUTE').groupBy('ROUTE').count()
    busyest_route_single = busyest_route_single.join(df_unique, busyest_route_single.ROUTE == df_unique.ROUTE_UNIQUE)
    busyest_route_single = busyest_route_single.drop('ROUTE_UNIQUE')
    busyest_route_single = busyest_route_single.sort(desc('count'))

    busyest_route_single.show()

    busyest_route_single.toPandas().to_csv('Output/MostBussyRoute.csv')

if __name__ == "__main__":
    sc = SparkContext()
    main(sc)
