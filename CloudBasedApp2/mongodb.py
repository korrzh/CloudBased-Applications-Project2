from pymongo import MongoClient
import csv
import time

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
print(f"Inserted {len(result.inserted_ids)} documents into MongoDB.")

print("For all valid information (i.e. sensor quality >= 0.95), what is the average temperature per year?")

start = time.time()
pipeline = [
    {"$match": {"sensorQuality": {"$gte": 0.95}}},
    {"$group": {"_id": "$Year","averageTempPerYear": {"$avg": "$measuredTemperature"}}}
]
end = time.time()

result = collection.aggregate(pipeline)

for doc in result:
    print(doc)

print("Execution time:", round(end - start, 6))

time.sleep(5)

# ##############################################################
print("For every year in the dataset, find the station ID with the highest / lowest temperature.")

# returns 1 station with maximum/minimum temperature per year
start = time.time()
pipeline = [
    {"$sort": {"Year": 1, "measuredTemperature": 1}},
    {"$group": {
        "_id": "$Year",
        "minTemp": {"$first": "$measuredTemperature"},
        "minStationId": {"$first": "$StationId"},
        "maxTemp": {"$last": "$measuredTemperature"},
        "maxStationId": {"$last": "$StationId"}
    }}
]

result = collection.aggregate(pipeline)

end = time.time()

for doc in result:
    print(doc)

print("Execution time:", end - start)

time.sleep(5)
##############################################################
# returns all stations that reached a maximum or minimum for the year, without duplication

start = time.time()

pipelineMaxStationTemp = [
    {
        "$group": {"_id": "$Year","maxTemp": {"$max": "$measuredTemperature"}}
    }
]

maxStationTemp = list(collection.aggregate(pipelineMaxStationTemp))

result = []

for row in maxStationTemp:
    year = row["_id"]
    maxTemp = row["maxTemp"]

    stations = list(collection.find(
        {"Year": year, "measuredTemperature": maxTemp,},
        {"_id": 0, "StationId": 1}
    ))

    for station in stations:
        result.append({"Year": year, "StationId": station["StationId"],"measuredTemperature": maxTemp})

end = time.time() 

unique_result = {
    (r["Year"], r["StationId"], r["measuredTemperature"]): r
    for r in result
}.values()


for doc in sorted(unique_result, key=lambda x: (x["Year"], x["StationId"])):
    print(doc)

print("Execution time:", end - start)

time.sleep(5)

start = time.time()

pipelineMinStationTemp = [
    {
        "$group": {"_id": "$Year","minTemp": {"$min": "$measuredTemperature"}}
    }
]

minStationTemp = list(collection.aggregate(pipelineMinStationTemp))

result = []

for row in minStationTemp:
    year = row["_id"]
    minTemp = row["minTemp"]

    stations = list(collection.find(
        {"Year": year, "measuredTemperature": minTemp,},
        {"_id": 0, "StationId": 1}
    ))

    for station in stations:
        result.append({"Year": year, "StationId": station["StationId"],"measuredTemperature": minTemp})

end = time.time()

unique_result = {
    (r["Year"], r["StationId"], r["measuredTemperature"]): r
    for r in result
}.values()


for doc in sorted(unique_result, key=lambda x: (x["Year"], x["StationId"])):
    print(doc)


print("Execution time:", end - start)

time.sleep(5)

###############################################################
print("For every year in the dataset, find the station ID with the highest maximal temperature for all stations with sensor quality >= 0.95.")

start = time.time()

pipelineMaxStationTemp = [
        {"$match": {"sensorQuality": {"$gte": 0.95}}},
        {"$group": {"_id": "$Year","maxTemp": {"$max": "$measuredTemperature"}}}
]

maxStationTemp = list(collection.aggregate(pipelineMaxStationTemp))

result = []

for row in maxStationTemp:
    year = row["_id"]
    maxTemp = row["maxTemp"]

    stations = list(collection.find(
        {"Year": year, "measuredTemperature": maxTemp, "sensorQuality": {"$gte": 0.95}},
        {"_id": 0, "StationId": 1}
    ))

    for station in stations:
        result.append({"Year": year, "StationId": station["StationId"],"measuredTemperature": maxTemp})

unique_result = {
    (r["Year"], r["StationId"], r["measuredTemperature"]): r
    for r in result
}.values()

end = time.time()

for doc in sorted(unique_result, key=lambda x: (x["Year"], x["StationId"])):
    print(doc)

print("Execution time:", end - start)
