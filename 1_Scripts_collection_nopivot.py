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


def parseCSV3(idx, part):
	if idx==0:
		part.next()
	for p in csv.reader(part):
		if p[0] == '2014':
			yield Row(YEAR = p[0],
                      MONTH = int(p[2]),
                      ORIGIN_CITY_NAME = p[15],
                      ORIGIN_AIRPORT_ID = p[11],
                      DEST_CITY_NAME = p[24],
                      DEST_AIRPORT_ID = p[20])
		elif p[0] == '2015':
			yield Row(YEAR = p[0],
                      MONTH = int(p[2])+12,
                      ORIGIN_CITY_NAME = p[15],
                      ORIGIN_AIRPORT_ID = p[11],
                      DEST_CITY_NAME = p[24],
                      DEST_AIRPORT_ID = p[20])
		elif p[0] == '2016':
			yield Row(YEAR = p[0],
                      MONTH = int(p[2])+24,
                      ORIGIN_CITY_NAME = p[15],
                      ORIGIN_AIRPORT_ID = p[11],
                      DEST_CITY_NAME = p[24],
                      DEST_AIRPORT_ID = p[20])
		else:
			pass

def parseCSV4(idx, part):
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

def parseCSV5(idx, part):
	if idx==0:
		part.next()
	for p in csv.reader(part):
		if p[0] == '2014':
			yield Row(YEAR = p[0],
                      MONTH = int(p[2]),
                      DEP_TIME_BLK = p[35],
                      DEST_CITY_NAME = p[24])
		elif p[0] == '2015':
			yield Row(YEAR = p[0],
                      MONTH = int(p[2])+12,
                      DEP_TIME_BLK = p[35],
                      DEST_CITY_NAME = p[24])
		elif p[0] == '2016':
			yield Row(YEAR = p[0],
                      MONTH = int(p[2])+24,
                      DEP_TIME_BLK = p[35],
                      DEST_CITY_NAME = p[24])
		else:
			pass

def main(sc):
	spark = HiveContext(sc)
	sqlContext = HiveContext(sc)
	#busiestcity
	rows = sc.textFile('../lmf445/Flight_Project/Data/864625436_T_ONTIME_2*.csv').mapPartitionsWithIndex(parseCSV3)
	df = sqlContext.createDataFrame(rows)
	flight_origin = df.select('YEAR', 'MONTH', 'ORIGIN_CITY_NAME', 'ORIGIN_AIRPORT_ID').groupBy('YEAR', 'MONTH', 'ORIGIN_CITY_NAME').count().withColumnRenamed('count', 'origin_count')
	flight_origin = flight_origin.withColumnRenamed('ORIGIN_CITY_NAME', 'City_of_Departure')

	flight_dest = df.select('YEAR', 'MONTH', 'DEST_CITY_NAME', 'DEST_AIRPORT_ID').groupBy('YEAR', 'MONTH', 'DEST_CITY_NAME').count().withColumnRenamed('count', 'dest_count')
	flight_dest = flight_dest.withColumnRenamed('DEST_CITY_NAME', 'City_of_Arrival')
	flight_dest = flight_dest.withColumnRenamed('YEAR', 'YEAR_dest')
	flight_dest = flight_dest.withColumnRenamed('MONTH', 'MONTH_dest')
	total_counts = flight_origin.join(flight_dest,((flight_origin.City_of_Departure == flight_dest.City_of_Arrival) & (flight_origin.YEAR == flight_dest.YEAR_dest) & (flight_origin.MONTH == flight_dest.MONTH_dest)))
	total_counts = total_counts.select(total_counts.City_of_Departure.alias('City'),(total_counts.origin_count + total_counts.dest_count).alias('sum_counts'), 'YEAR', 'MONTH')
	total_counts_city_pivot = total_counts.groupBy('City').pivot('MONTH').sum('sum_counts')
	total_counts_city_pivot.toPandas().to_csv('Output/busiest_city.csv')
	#grupedbyday
	rows = sc.textFile('../lmf445/Flight_Project/Data/864625436_T_ONTIME_2*.csv').mapPartitionsWithIndex(parseCSV4)
	df = sqlContext.createDataFrame(rows)
	grouped_by_day = df.groupBy('FL_DATE').count()
	grouped_by_day.toPandas().to_csv('Output/grouped_by_day.csv')
	#mostcommondeparturetime
	rows = sc.textFile('../lmf445/Flight_Project/Data/864625436_T_ONTIME_*.csv').mapPartitionsWithIndex(parseCSV5)
	df = sqlContext.createDataFrame(rows)
	departure_time_pivot = df.groupBy('DEP_TIME_BLK').pivot('MONTH').count()
	departure_time_pivot.toPandas().to_csv('Output/most_common_departure_time.csv')

if __name__ == "__main__":
	sc = SparkContext()
	main(sc)