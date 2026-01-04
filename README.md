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
# MongoDB
   
## Execution Time Comparison

| Metric / Query                              | avgTemperaturePerYear | stationWithExtremeTemperaturesPerYear | stationsWithExtremeTemperaturesPerYear | stationWithHighestMaxPerYear | stationWithHighestStationMaxPerYear |
|---------------------------------------------|-----------------------|----------------------------------------|-----------------------------------------|------------------------------|-------------------------------------|
| PySpark Execution Time                      | TBD                   | TBD                                    | TBD                                     | TBD                          | TBD                                 |
| MongoDB Execution Time                      | TBD                   | TBD                                    | TBD                                     | TBD                          | TBD                                 |
