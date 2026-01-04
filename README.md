# CloudBased-Applications-Project2

### YouTube Link

This project provides scripts to analyze temperature data using two different tools: PySpark and MongoDB. The scripts answer three questions:

1. For all valid information (i.e. sensor quality >= 0.95), what is the average temperature per year? 
2. For every year in the dataset, find the station ID with the highest / lowest temperature. 
3. For every year in the dataset, find the station ID with the highest maximal temperature for all stations with 
    sensor quality >= 0.95.

The project uses `docker-compose` to orchestrate two services:
- **pyspark** — a container with PySpark and a Python environment;
- **mongo** — a MongoDB container.

## How to Run the Project
1. Clone the repository  
2. Navigate to the `.devcontainer` directory  
3. Run:
   ```bash
   docker-compose up --build

### Code Explanation 

# PySpark 
The entry point of the application initializes a Spark session, loads the input dataset, and prepares the data for analysis.
A Spark session is created using `SparkSession.builder`, after which the `temperature.tsv` file is read as a tab-separated dataset without a header. The raw data is then converted to a DataFrame with meaningful column names (`StationId`, `Year`, `measuredTemperature`, `sensorQuality`), and all columns are explicitly cast to the appropriate data types.
```
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
```
Query 1: first, I filter the data to keep only records with sensor quality greater than or equal to 0.95```(using df.filter)```.Then I group the data by year```(.groupBy("Year"))``` and compute the mean value ```.mean()``` of the measured temperature. Finally, I sort the result by year

Query 2: Since in the temperature.tsv file different stations can reach the same maximum or minimum temperatures in one year,and the task did not specify whether to return only one record-breaking station or the entire list, I implemented two functions

- **def stationWithExtremeTemperaturesPerYear** - returns 1 station with maximum/minimum temperature per year

First, I define two window specifications partitioned by year (`Window.partitionBy`) and ordered by temperature in descending and ascending order (`orderBy`). Then, using window functions (`first`), I select the station ID and temperature for both the maximum and minimum values within each year. Finally, I select the relevant columns (`select`), remove duplicates (`distinct`), and sort the result by year (`orderBy`)

- **def stationsWithExtremeTemperaturesPerYear** - returns all stations that reached a maximum or minimum for the year, without duplication

First, I compute the maximum and minimum temperature for each year using grouping and aggregation (`groupBy`, `agg`, `max`, `min`).Then, I join these results back with the original DataFrame to find all stations that recorded those extreme values (`join`, `filter`)

Query 3:
Here I was not sure what query I needed to implement:
- **def stationWithHighestMaxPerYear** - take all measurements with sensorQuality ≥ 0.95, find the maximum temperature for the year, and return all stations that recorded it

First, I find the maximum temperature per year using grouping and agregation (`groupBy`, `agg`, `max`). Then, I join this result with the filtered dataset (`join`) to select the statios that recorded this maximm temperature


- **def stationWithHighestStationMaxPerYear** - take all measurements with sensorQuality ≥ 0.95, for each station find its maximum for the year, and then among these maximal select the highest

First, I compute the maximum temperature per station for each year (`groupBy` by year and station, `max`). Then, I select the highest of these station-level maxima for each year (`groupBy`, `max`). Finally, I join the results to retrieve the stations with the highest yearly maximum temperature and remove duplicates (`join`, `dropDuplicates`)

# MongoDB

First, a connection to the MongoDB container is established using `MongoClient`, and the target database and collection are selected. Then I imported data

```
client = MongoClient("mongodb://mongo:27017/")
db = client["temperatureDb"]  
collection = db["sensorData"]

with open("/app/temperature.tsv", "r") as file:
    reader = csv.DictReader(file, delimiter="\t", fieldnames=["StationId", "Year", "measuredTemperature", "sensorQuality"])
    data = []

    for row in reader:
        data.append({
            "StationId": int(row["StationId"]),
            "Year": int(row["Year"]),
            "measuredTemperature": float(row["measuredTemperature"]),
            "sensorQuality": float(row["sensorQuality"])
        })

result = collection.insert_many(data)
```
Query 1: first, I filter the documents to keep only records with sensor quality greater than or equal to 0.95 (`$match`). Then, I group the data by year(`$group` with `_id: "$Year"`) and compute the average value of the measured temperature using `$avg`

Query 2.1: first, I sort the docments by year and temperature (`$sort`). Then, I group the data by year (`$group`) and use `$first` and `$last` to select the stations with the minimum and maximum temperatures for each year

Query 2.2: first, I find the maximum and minimum temperature per year using aggregation (`$group` with `$max` / `$min`). Then, for each year, I query the collection to retrieve all stations that recorded these extreme values (`find`)


Query 3: Similar to query 2.2, but filters by valid data (Sensor_quality >= 0.95)

Note: I did not implement a separate MongoDB version of `stationWithHighestStationMaxPerYear`, because in practice it returns the same results as `stationWithHighestMaxPerYear`.

## Execution Time Comparison

| Metric / Query                              | avgTemperaturePerYear | stationWithExtremeTemperaturesPerYear | stationsWithExtremeTemperaturesPerYear | stationWithHighestMaxPerYear | stationWithHighestStationMaxPerYear |
|---------------------------------------------|-----------------------|----------------------------------------|-----------------------------------------|------------------------------|-------------------------------------|
| PySpark Execution Time                      | 0.173s                      | 0.291s                                   | 0.275s                                    | 0.210s                          | 0.309s                                 |
| MongoDB Execution Time                      | 6e-06s                      | 0.149s                                   | 0.607s                                    | 0.210s                          | ---                                    |

### Conclusions

From the table above, we can see that MongoDB queries are generally fast for simple aggregations (like `avgTemperaturePerYear`), but more complex queries (like finding all stations with extreme temperatures) take longer than PySpark  

In practice, the MongoDB queries could be further optimized—for example, there is no direct equivalent of `dropDuplicates` in MongoDB, and some aggregation pipelines could be rewritten to reduce multiple queries. However, I chose the current implementation because it is easier to understand and read, which makes the logic clearer

**When to use each tool:**  
- **PySpark** is better for large datasets, complex transformations, and operations that require distributed computing 
- **MongoDB** is convenient for simpler aggregations,or when the data is already stored in a document database, especially for small-to-medium datasets.

