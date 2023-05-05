import requests
import pymongo
import json

# globalfishingwatch.py -- get fishing events from Global Fishing Watch API and insert into Mongo

# Global Fishing Watch Key 
apiKey = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6ImtpZEtleSJ9.eyJkYXRhIjp7Im5hbWUiOiI2NTEzIEJpZyBEYXRhIiwidXNlcklkIjoyNDg5NywiYXBwbGljYXRpb25OYW1lIjoiNjUxMyBCaWcgRGF0YSIsImlkIjo1NzIsInR5cGUiOiJ1c2VyLWFwcGxpY2F0aW9uIn0sImlhdCI6MTY4MTc3NDU4NSwiZXhwIjoxOTk3MTM0NTg1LCJhdWQiOiJnZnciLCJpc3MiOiJnZncifQ.Yq_OY9ZAGa2svMG8MCbxZbrDaCjkY3j5sjHVngxSdSUCeN4mDsILjvsb9oUrWdpC52kGDzE2HhvNkqbLkrt4H6LtI2SfijS6Brj1v8dpVqy7dQkshGlPClvGHUWnBlhTyCtd3nl5VpCbHMtDpfewI8AHWtqnqnrIGE0ElI3EmVM5VqonFuG_kIavpGraw47akDp-rwcTJSSZIr-QQeOme6u2fqCzGa5J08FH-MlrtqSeT8ewfBFD34qGHOJ3XdsjX_HgpIcsxCBcxnWIsP_YohHLixVnueRNct6tJpZ66xQYp3s8dNgZSIcr4nJNxxr_PVBonHBs5FWsRDdbXzsoNEluYPH9eGUzg4o6aHO9vqpDBwM0sngu03ewiRrLc010h0CAprve6s4nKKfw9qpTu3T6JIUwuDfzHeASVcA0a43PRsUEMuN5Tcn-5UAkUev5jzWUj4u6VAMBWVIdhvotJR8oNVmV2QXqYPo9833_zYIYL6JP9cr7wBRA_BJ4hFXk'
# gfwUrl = 'https://api.globalfishingwatch.org/v2/events'
# Not sure if there's a better way to set this limit or to get around it
gfwUrl = 'https://gateway.api.dev.globalfishingwatch.org/v2/events?offset=0&limit=999999999'

# Make POST request to GFW API over given month and coords
def getFishingEvents(month, lat, long, collection):
    # Request params
    # set endDay to number of days in month
    endDay = 31
    # Set endDay to number of days in month
    if month == "02":
        endDay = 28
    elif month == "04" or month == "06" or month == "09" or month == "11":
        endDay = 30
    headers = {"Authorization": f"Bearer {apiKey}", "Content-Type": "application/json"}
    params = {
        "datasets": ["public-global-fishing-events:latest"],
        "startDate": "2022-" + str(month) + "-01",
        "endDate": "2022-" + str(month) + "-" + str(endDay),
        "geometry": {
            "type": "Polygon",
            # "coordinates": [[[-74.5, 39.5], [-72, 39.5], [-72, 42], [-74.5, 42], [-74.5, 39.5]]]
            "coordinates": [[[long, lat], [long+1, lat], [long+1, lat+1], [long, lat+1], [long, lat]]]
            # Get coordintes 
        }
    }
    # print(params)
    response = requests.post(gfwUrl, headers=headers, data=json.dumps(params))
    responseJson = None
    if response.status_code == 200 or response.status_code == 201:
        responseJson = response.json()
        # Upload each record in responseJson to mongo, using current lat long month as key
        for record in responseJson["entries"]:
            collection.insert_one({"lat": lat, "long": long, "month": month, "record": record})
        return len(responseJson["entries"])
    else:
        print("Error:", response.json())
    # Print response
    # print(len(responseJson["entries"]))
    # print(responseJson)
    # return len(responseJson["entries"])
    return None


# Connect to Mongo
client = pymongo.MongoClient("mongodb+srv://theresatvan:UEi8751OX1jaT9lz@cluster0.6jfc5iw.mongodb.net/")
mydatabase = client["gfw"]
mycollection = mydatabase["fishing"]
mycollection.delete_many({})
# Get fishing events for each month of 2022 for each 1 latitude by 1 longitude square on the globe
# f = open("output2.txt", "w")
center = (41, -74) # NYC
radius = 10
# Iterate through all lat/long squares in radius of center
for lat in range(center[0]-radius, center[0]+radius):
    for long in range(center[1]-radius, center[1]+radius):
        # Get fishing events for each month
        months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11"]
        for month in months:
            getFishingEvents(month, lat, long, mycollection)
            # print("starting lat: " + str(lat) + ", starting long: " + str(long) + ", month: " + str(month) + ", num events: " + str(getFishingEvents(month, lat, long)), file=f)
# f.close()
client.close()



# Insert fishing events into Mongo

