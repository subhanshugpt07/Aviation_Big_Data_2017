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
		if p[14] == 'ORIGIN':
			pass
		else:
			yield Row(FL_DATE = p[5],
	                      ORIGIN=p[14],
	                      ORIGIN_AIRPORT_ID = p[11],
	                      DEST = p[23],
	                      DEST_AIRPORT_ID = p[20])

def main(sc):
	spark = HiveContext(sc)
	sqlContext = HiveContext(sc)
	rows = sc.textFile('../lmf445/Flight_Project/Data/864625436_T_ONTIME_2*.csv').mapPartitionsWithIndex(parseCSV)
	df = sqlContext.createDataFrame(rows)

	flight_origin = df.select('FL_DATE', 'ORIGIN', 'ORIGIN_AIRPORT_ID')\
		.groupBy('FL_DATE', 'ORIGIN') \
	    .count() \
	    .withColumnRenamed('count', 'origin_count')
	flight_origin = flight_origin.withColumnRenamed('ORIGIN', 'Airport_origin')

	flight_dest = df.select('FL_DATE', 'DEST', 'DEST_AIRPORT_ID') \
	    .groupBy('FL_DATE', 'DEST') \
	    .count() \
	    .withColumnRenamed('count', 'dest_count')

	flight_dest = flight_dest.withColumnRenamed('DEST', 'Airport_dest')
	flight_dest = flight_dest.withColumnRenamed('FL_DATE', 'FL_DATE_dest')

	total_counts = \
	    flight_origin.join(flight_dest,
	    ((flight_origin.Airport_origin==flight_dest.Airport_dest) &
	    (flight_origin.FL_DATE==flight_dest.FL_DATE_dest)))

	total_counts = \
	    total_counts.select(total_counts.Airport_origin.alias('Airport'),
	    (total_counts.origin_count+total_counts.dest_count).alias('sum_counts'),
	    'FL_DATE')\

	total_counts_airport_pivot = \
	    total_counts.groupBy('Airport').pivot('FL_DATE').sum('sum_counts')

	total_counts_airport_pivot.toPandas().to_csv('Output/busiest_airport_by_day.csv')

if __name__ == "__main__":
    sc = SparkContext()
    main(sc)
