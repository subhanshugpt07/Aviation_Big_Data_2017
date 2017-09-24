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
        if p[14] < p[23]:
            if p[0] == '2014':
                yield Row(YEAR = p[0],
                          MONTH = int(p[2]),
                          ORIGIN=p[14],
                          ORIGIN_AIRPORT_ID = p[11],
                          DEST = p[23],
                          DEST_AIRPORT_ID = p[20],
                          ROUTE = (p[14],p[23]))
            elif p[0] == '2015':
                yield Row(YEAR = p[0],
                          MONTH = int(p[2])+12,
                          ORIGIN=p[14],
                          ORIGIN_AIRPORT_ID = p[11],
                          DEST = p[23],
                          DEST_AIRPORT_ID = p[20],
                          ROUTE = (p[14],p[23]))
            elif p[0] == '2016':
                yield Row(YEAR = p[0],
                          MONTH = int(p[2])+24,
                          ORIGIN=p[14],
                          ORIGIN_AIRPORT_ID = p[11],
                          DEST = p[23],
                          DEST_AIRPORT_ID = p[20],
                          ROUTE = (p[14],p[23]))
            else:
                pass
        else:
            if p[0] == '2014':
                yield Row(YEAR = p[0],
                          MONTH = int(p[2]),
                          ORIGIN=p[23],
                          ORIGIN_AIRPORT_ID = p[11],
                          DEST = p[14],
                          DEST_AIRPORT_ID = p[20],
                          ROUTE = (p[23],p[14]))
            elif p[0] == '2015':
                yield Row(YEAR = p[0],
                          MONTH = int(p[2])+12,
                          ORIGIN=p[23],
                          ORIGIN_AIRPORT_ID = p[11],
                          DEST = p[14],
                          DEST_AIRPORT_ID = p[20],
                          ROUTE = (p[23],p[14]))
            elif p[0] == '2016':
                yield Row(YEAR = p[0],
                          MONTH = int(p[2])+24,
                          ORIGIN=p[23],
                          ORIGIN_AIRPORT_ID = p[11],
                          DEST = p[14],
                          DEST_AIRPORT_ID = p[20],
                          ROUTE = (p[23],p[14]))
            else:
                pass

def main(sc):
    spark = HiveContext(sc)
    sqlContext = HiveContext(sc)
    print "holaaaaa"
    rows = sc.textFile('../lmf445/Flight_Project/Data/864625436_T_ONTIME_2*.csv').mapPartitionsWithIndex(parseCSV)
    df = sqlContext.createDataFrame(rows)
    busiest_route_month_pivot = \
        df.select('ORIGIN_AIRPORT_ID', 'ROUTE', 'MONTH') \
            .groupBy('ROUTE').pivot('MONTH').count()

    busiest_route_month_pivot.toPandas().to_csv('Output/MonthlyRoutes.csv')


if __name__ == "__main__":
    sc = SparkContext()
    main(sc)
# In[ ]: