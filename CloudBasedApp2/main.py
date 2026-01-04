from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, mean, max, min, first, last
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.window import Window
import time

# For all valid information (i.e. sensor quality >= 0.95), what is the average temperature per year?
def avgTemperaturePerYear(df):
    return df.filter(col("sensorQuality") >= 0.95) \
        .groupBy("Year") \
        .mean("measuredTemperature") \
        .orderBy("Year")

###############################################################
# For every year in the dataset, find the station ID with the highest / lowest temperature.

# Since in the temperature.tsv file different stations can reach the same maximum or minimum temperatures in one year, 
# and the task did not specify whether to return only one record-breaking station or the entire list, I implemented two functions
def stationWithExtremeTemperaturesPerYear(df):
    # returns 1 station with maximum/minimum temperature per year
    w_max = Window.partitionBy("Year").orderBy(col("measuredTemperature").desc())
    w_min = Window.partitionBy("Year").orderBy(col("measuredTemperature").asc())

    return df \
        .withColumn("maxStationId", first("StationId").over(w_max)) \
        .withColumn("maxTemp", first("measuredTemperature").over(w_max)) \
        .withColumn("minStationId", first("StationId").over(w_min)) \
        .withColumn("minTemp", first("measuredTemperature").over(w_min)) \
        .select("Year", "maxStationId", "maxTemp", "minStationId", "minTemp") \
        .distinct() \
        .orderBy("Year")

def stationsWithExtremeTemperaturesPerYear(df):
    # returns all stations that reached a maximum or minimum for the year, without duplication
    yearMaxTemp = df.groupBy("Year") \
                    .agg(max("measuredTemperature").alias("maxTemp"))

    yearMinTemp = df.groupBy("Year") \
                    .agg(min("measuredTemperature").alias("minTemp"))

    maxStations = df.join(yearMaxTemp, on="Year") \
                    .filter(col("measuredTemperature") == col("maxTemp")) \
                    .select("Year", col("StationId").alias("stationWithMaxTemp"), "measuredTemperature") \
                    .dropDuplicates(["Year", "stationWithMaxTemp", "measuredTemperature"]) \

    minStations = df.join(yearMinTemp, on="Year") \
                    .filter(col("measuredTemperature") == col("minTemp")) \
                    .select("Year", col("StationId").alias("stationWithMinTemp"), "measuredTemperature") \
                    .dropDuplicates(["Year", "stationWithMinTemp", "measuredTemperature"]) \

    return maxStations, minStations      

###############################################################
# For every year in the dataset, find the station ID with the highest maximal temperature for all stations with sensor quality >= 0.95.
# Here I was not sure what query I needed to implement:
# 1)take all measurements with sensorQuality ≥ 0.95, find the maximum temperature for the year, and return all stations that recorded it
# 2)take all measurements with sensorQuality ≥ 0.95, for each station find its maximum for the year, and then among these maximal select the highest
#In fact, in this case both approaches return the same result
def stationWithHighestMaxPerYear(df):
    yearMaxTemp = df.filter(col("sensorQuality") >= 0.95) \
                    .groupBy("Year") \
                    .agg(max("measuredTemperature").alias("maxTemp"))

    maxStations = df.filter(col("sensorQuality") >= 0.95) \
                    .join(yearMaxTemp, on="Year") \
                    .filter(col("measuredTemperature") == col("maxTemp")) \
                    .select("Year", col("StationId").alias("stationWithHighestMaxTemp"),"measuredTemperature", "sensorQuality") \
                    .dropDuplicates(["Year", "stationWithHighestMaxTemp", "measuredTemperature", "sensorQuality"]) \

    return maxStations

def stationWithHighestStationMaxPerYear(df):
    stationYearMaxTemp = df.filter(col("sensorQuality") >= 0.95) \
                            .groupBy("Year", "StationId") \
                            .agg(max("measuredTemperature").alias("stationMaxTemp"))

    yearHighestStationMax = stationYearMaxTemp.groupBy("Year") \
                                               .agg(max("stationMaxTemp").alias("maxTemp"))

    maxStations = stationYearMaxTemp.join(yearHighestStationMax, on="Year") \
                                    .filter(col("stationMaxTemp") == col("maxTemp")) \
                                    .select(
                                        "Year",
                                        col("StationId").alias("stationWithHighestMaxTemp"),
                                        col("stationMaxTemp").alias("maxTemp")
                                    ) \
                                    .dropDuplicates(["Year", "stationWithHighestMaxTemp", "maxTemp"]) \

    return maxStations

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("DF-Example") \
        .getOrCreate()

    file = "temperature.tsv"

    df = spark.read \
        .option("header", "false") \
        .option("sep", "\t") \
        .csv(file) \
        .toDF("StationId", "Year", "measuredTemperature", "sensorQuality")

    df = df \
        .withColumn("Year", col("Year").cast(IntegerType())) \
        .withColumn("measuredTemperature", col("measuredTemperature").cast(DoubleType())) \
        .withColumn("sensorQuality", col("sensorQuality").cast(DoubleType()))


    print("Task 1")
    start = time.time()

    stationsAverageTemperature = avgTemperaturePerYear(df)

    end = time.time()

    stationsAverageTemperature.show()

    print("Execution time:", end - start)

    time.sleep(5)

    print("Task 2.1")

    start = time.time()

    stationExtremeTemperatures = stationWithExtremeTemperaturesPerYear(df)

    end = time.time()

    stationExtremeTemperatures.show()

    print("Execution time:", end - start)

    time.sleep(5)

    print("Task 2.2")

    start = time.time()

    maxStations, minStations = stationsWithExtremeTemperaturesPerYear(df)

    end = time.time()

    print("Stations with MAX temperature per year")
    for row in maxStations.orderBy("Year", "StationId").collect():
        print(row.asDict())


    print("Stations with MIN temperature per year")
    for row in minStations.orderBy("Year", "StationId").collect():
        print(row.asDict())


    print("Execution time:", end - start)

    time.sleep(5)

    print("Task 3.1")

    start = time.time()

    stationWithHighestTemp = stationWithHighestMaxPerYear(df)

    end = time.time()

    for row in stationWithHighestTemp.orderBy("Year", "StationId").collect():
        print(row.asDict())

    print("Execution time:", end - start)

    time.sleep(5)

    print("Task 3.2")

    start = time.time()

    stationWithHighestTemp2 = stationWithHighestStationMaxPerYear(df)

    end = time.time()

    for row in stationWithHighestTemp2.orderBy("Year", "StationId").collect():
        print(row.asDict())

    print("Execution time:", end - start)
    
