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
    if idx == 0:
        part.next()
    for p in csv.reader(part):
        yield Row(ORIGIN=p[14],
                  DEST=p[23],
                  # ROUTE = (p[11],p[20]),
                  ROUTE=(p[14], p[23]),
                  ARR_DEL15=p[44])

def main(sc):
    spark = HiveContext(sc)
    sqlContext = HiveContext(sc)
    rows = sc.textFile('../lmf445/Flight_Project/Data/864625436_T_ONTIME_*.csv')
    df = sqlContext.createDataFrame(rows)
    first = df.withColumn('ARR_DEL15', df['ARR_DEL15'].cast('int'))
    sec = first.na.drop()
    third = sec.select('ROUTE', 'ARR_DEL15', 'DEST', 'ORIGIN').filter(sec.ARR_DEL15 == 1).groupby(
        'ROUTE').count().withColumnRenamed('count', 'delay_count')
    fourth = third.sort(desc('count'))
    fifth = sec.select('ROUTE', 'ARR_DEL15', 'DEST', 'ORIGIN').groupby('ROUTE').count()
    sixth = fifth.sort(desc('count'))
    eighth = fourth.join(sixth, 'ROUTE')
    ninth = eighth.sort(desc('count'))
    tenth = ninth.select('ROUTE', ninth['delay_count'] / ninth['count'])
    eleventh = tenth.sort(desc('(delay_count / count)'))
    twelveth = eleventh.join(eighth, 'ROUTE')
    thirteen = twelveth.sort(desc('(delay_count / count)'))
    thirteen.toPandas().to_csv('Output/route_delayone.csv')

if __name__ == "__main__":
    sc = SparkContext()
    main(sc)
# In[ ]: