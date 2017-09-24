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


def parseCSV1(idx, part):
    if idx==0:
        part.next()
    for p in csv.reader(part):
        if p[8] != 'NULL' :
            if p[8] == 'CARRIER':
                pass
            else:
                yield Row(
                      CARRIER = p[8],
                      AIRLINE_ID = p[7],
                      DEST_AIRPORT_ID = int(p[20]))

def parseCSV2(idx, part):
    if idx==0:
        part.next()
    #count = 0
    for p in csv.reader(part):
        if p[8] != 'NULL' :
            yield Row(CARRIER=p[8],
                      DEP_DEL15 = p[33],
                      DEP_DELAY_NEW = p[32])

def parseCSV3(idx, part):
    if idx==0:
        part.next()
    count = 0
    for p in csv.reader(part):
        if p[8] != 'NULL' :
            if p[8] == 'CARRIER':
                pass
            else:
                yield Row(CARRIER=p[8],
                      CARRIER_DELAY=p[56],
                      NAS_DELAY=p[58],
                      SECURITY_DELAY=p[59],
                      WEATHER_DELAY = p[57],
                      LATE_AIRCRAFT_DELAY = p[60],
                      DEP_DELAY_NEW = p[32])

def parseCSV4(idx, part):
    if idx==0:
        part.next()
    count = 0
    for p in csv.reader(part):
        if p[8] != 'NULL' :
            yield Row(CARRIER=p[8],
                      CARRIER_DELAY=p[56],
                      NAS_DELAY=p[58],
                      SECURITY_DELAY=p[59],
                      WEATHER_DELAY = p[57],
                      LATE_AIRCRAFT_DELAY = p[60],
                      DEP_DELAY_NEW = p[32])

def parseCSV5(idx, part):
    if idx==0:
        part.next()
    for p in csv.reader(part):
        if p[8] != 'NULL' :
            if p[8]=='CARRIER':
                pass
            else:
                yield Row(
                      CARRIER = p[8],
                      AIRLINE_ID = p[7],
                      DEST_AIRPORT_ID = str(p[20]))

def main(sc):
    spark = HiveContext(sc)
    sqlContext = HiveContext(sc)
    # airline_most_departure_delays
    rows = sc.textFile('../lmf445/Flight_Project/Data/864625436_T_ONTIME_2*.csv').mapPartitionsWithIndex(parseCSV1)
    df = sqlContext.createDataFrame(rows)
    airl_origin= df.select('CARRIER', 'AIRLINE_ID').groupBy('CARRIER').count().withColumnRenamed('count', 'airlineId_count')
    airl_origin = airl_origin.withColumnRenamed('CARRIER', 'Airline_origin')
    airl_origin.toPandas().to_csv('Output/airline_most_departure_delays_new.csv')
    # airline_with_maximum_delay_new
    rows = sc.textFile('../lmf445/Flight_Project/Data/864625436_T_ONTIME_2*.csv').mapPartitionsWithIndex(parseCSV2)
    da = sqlContext.createDataFrame(rows)
    db = da.select('CARRIER', 'DEP_DEL15').where(da.DEP_DEL15 == 1).groupby('CARRIER').count().withColumnRenamed(
		'count', 'delay_count')
    db = db.na.drop()
    dp = da.select('CARRIER', 'DEP_DEL15', 'DEP_DELAY_NEW').where(da.DEP_DEL15 == 0).groupby(
		'CARRIER').count().withColumnRenamed('count', 'not_delay_count')
    dp = dp.na.drop()
    dp.toPandas().to_csv('Output/airline_with_maximum_delay_new.csv')
    # most_common_reason_for_delay
    rows = sc.textFile('../lmf445/Flight_Project/Data/864625436_T_ONTIME_2*.csv').mapPartitionsWithIndex(parseCSV3)
    d = sqlContext.createDataFrame(rows)
    d_c = d.select('CARRIER', 'CARRIER_DELAY').filter(d.CARRIER_DELAY > 0).groupby('CARRIER').count().withColumnRenamed(
		'count', 'CARRIER_DELAY_count')
    d_n = d.select('CARRIER', 'NAS_DELAY').filter(d.NAS_DELAY > 0).groupby('CARRIER').count().withColumnRenamed('count',
																										'NAS_DELAY_count')
    d_s = d.select('CARRIER', 'SECURITY_DELAY').filter(d.SECURITY_DELAY > 0).groupby('CARRIER').count().withColumnRenamed(
		'count', 'SECURITY_DELAY_count')
    d_w = d.select('CARRIER', 'WEATHER_DELAY').filter(d.WEATHER_DELAY > 0).groupby('CARRIER').count().withColumnRenamed(
		'count', 'WEATHER_DELAY_count')
    d_l = d.select('CARRIER', 'LATE_AIRCRAFT_DELAY').filter(d.LATE_AIRCRAFT_DELAY > 0).groupby(
		'CARRIER').count().withColumnRenamed('count', 'LATE_AIRCRAFT_DELAY_count')
    delay_rn_count = d_n.join(d_c, ["CARRIER"])
    delay_rn_count = delay_rn_count.join(d_s, ["CARRIER"], how='outer')
    delay_rn_count = delay_rn_count.join(d_w, ["CARRIER"], how='outer')
    delay_rn_count = delay_rn_count.join(d_l, ["CARRIER"], how='outer')
    delay_rn_count.toPandas().to_csv('Output/most_common_reason_for_delay_new.csv')
    # total_min_of_each_delay
    rows = sc.textFile('../lmf445/Flight_Project/Data/864625436_T_ONTIME_2*.csv').mapPartitionsWithIndex(parseCSV4)
    da = sqlContext.createDataFrame(rows)
    da = da.withColumn('CARRIER_DELAY', da['CARRIER_DELAY'].cast('int'))
    da = da.withColumn('NAS_DELAY', da['NAS_DELAY'].cast('int'))
    da = da.withColumn('SECURITY_DELAY', da['SECURITY_DELAY'].cast('int'))
    da = da.withColumn('WEATHER_DELAY', da['WEATHER_DELAY'].cast('int'))
    da = da.withColumn('LATE_AIRCRAFT_DELAY', da['LATE_AIRCRAFT_DELAY'].cast('int'))
    d_c_s = da.select('CARRIER', 'CARRIER_DELAY').filter(da.CARRIER_DELAY > 0).groupby('CARRIER').sum().withColumnRenamed(
		'sum(CARRIER_DELAY)', 'CARRIER_DELAY_sum')
    d_n_s = da.select('CARRIER', 'NAS_DELAY').filter(da.NAS_DELAY > 0).groupby('CARRIER').sum().withColumnRenamed(
		'sum(NAS_DELAY)', 'NAS_DELAY_sum')
    d_s_s = da.select('CARRIER', 'SECURITY_DELAY').filter(da.SECURITY_DELAY > 0).groupby('CARRIER').sum().withColumnRenamed(
		'sum(SECURITY_DELAY)', 'SECURITY_DELAY_sum')
    d_w_s = da.select('CARRIER', 'WEATHER_DELAY').filter(da.WEATHER_DELAY > 0).groupby('CARRIER').sum().withColumnRenamed(
		'sum(WEATHER_DELAY)', 'WEATHER_DELAY_sum')
    d_l_s = da.select('CARRIER', 'LATE_AIRCRAFT_DELAY').filter(da.LATE_AIRCRAFT_DELAY > 0).groupby(
		'CARRIER').sum().withColumnRenamed('sum(LATE_AIRCRAFT_DELAY)', 'LATE_AIRCRAFT_DELAY_sum')
    delay_rn_sum = d_n_s.join(d_c_s, ["CARRIER"], how='outer')
    delay_rn_sum = delay_rn_sum.join(d_s_s, ["CARRIER"], how='outer')
    delay_rn_sum = delay_rn_sum.join(d_w_s, ["CARRIER"], how='outer')
    delay_rn_sum = delay_rn_sum.join(d_l_s, ["CARRIER"], how='outer')
    delay_rn_sum.toPandas().to_csv('Output/total_min_of_each_delay_new.csv')
    rows = sc.textFile('../lmf445/Flight_Project/Data/864625436_T_ONTIME_2*.csv').mapPartitionsWithIndex(parseCSV5)
    df1 = sqlContext.createDataFrame(rows)
    airl_origin= df1.select('CARRIER', 'AIRLINE_ID').groupBy('CARRIER').count().withColumnRenamed('count', 'airlineId_count')
    airl_origin = airl_origin.withColumnRenamed('CARRIER', 'Airline_origin')
    airl_origin.toPandas().to_csv('Output/number_of_flights_new.csv')

if __name__ == "__main__":
    sc = SparkContext()
    main(sc)