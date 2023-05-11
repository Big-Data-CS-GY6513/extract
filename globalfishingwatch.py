import requests
import pymongo
import json

# globalfishingwatch.py -- get fishing events from Global Fishing Watch API and insert into Mongo

# Global Fishing Watch Key 
apiKey = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6ImtpZEtleSJ9.eyJkYXRhIjp7Im5hbWUiOiI2NTEzIEJpZyBEYXRhIiwidXNlcklkIjoyNDg5NywiYXBwbGljYXRpb25OYW1lIjoiNjUxMyBCaWcgRGF0YSIsImlkIjo1NzIsInR5cGUiOiJ1c2VyLWFwcGxpY2F0aW9uIn0sImlhdCI6MTY4MTc3NDU4NSwiZXhwIjoxOTk3MTM0NTg1LCJhdWQiOiJnZnciLCJpc3MiOiJnZncifQ.Yq_OY9ZAGa2svMG8MCbxZbrDaCjkY3j5sjHVngxSdSUCeN4mDsILjvsb9oUrWdpC52kGDzE2HhvNkqbLkrt4H6LtI2SfijS6Brj1v8dpVqy7dQkshGlPClvGHUWnBlhTyCtd3nl5VpCbHMtDpfewI8AHWtqnqnrIGE0ElI3EmVM5VqonFuG_kIavpGraw47akDp-rwcTJSSZIr-QQeOme6u2fqCzGa5J08FH-MlrtqSeT8ewfBFD34qGHOJ3XdsjX_HgpIcsxCBcxnWIsP_YohHLixVnueRNct6tJpZ66xQYp3s8dNgZSIcr4nJNxxr_PVBonHBs5FWsRDdbXzsoNEluYPH9eGUzg4o6aHO9vqpDBwM0sngu03ewiRrLc010h0CAprve6s4nKKfw9qpTu3T6JIUwuDfzHeASVcA0a43PRsUEMuN5Tcn-5UAkUev5jzWUj4u6VAMBWVIdhvotJR8oNVmV2QXqYPo9833_zYIYL6JP9cr7wBRA_BJ4hFXk'
# gfwUrl = 'https://api.globalfishingwatch.org/v2/events'
# Not sure if there's a better way to set this limit or to get around it
gfwUrl = 'https://gateway.api.dev.globalfishingwatch.org/v2/events?offset=0&limit=999999999'
months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
# Constant dictionary for MRGID EEZ codes to country name, retreived from mrgid.r script
codes = [3293, 5668, 5669, 5670, 5672, 5673, 5674, 5675, 5676, 5677, 5678, 5679, 5680, 5681, 5682,
              5683, 5684, 5685, 5686, 5687, 5688, 5689, 5690, 5691, 5692, 5693, 5694, 5695, 5696, 5697,
              8308, 8309, 8310, 8311, 8312, 8313, 8314, 8315, 8316, 8317, 8318, 8319, 8320, 8321, 8322,
              8323, 8324, 8325, 8326, 8327, 8328, 8329, 8330, 8331, 8332, 8333, 8334, 8335, 8336, 8337,
              8338, 8339, 8340, 8341, 8342, 8343, 8345, 8346, 8347, 8348, 8349, 8350, 8351, 8352, 8353,
              8354, 8355, 8356, 8357, 8358, 8359, 8360, 8361, 8362, 8363, 8364, 8365, 8366, 8367, 8368,
              8369, 8370, 8371, 8372, 8373, 8374, 8375, 8376, 8378, 8379, 8455]
countries = ["Belgian Exclusive Economic Zone",
    "Dutch Exclusive Economic Zone",
    "German Exclusive Economic Zone",
    "Albanian Exclusive Economic Zone",
    "Bulgarian Exclusive Economic Zone",
    "Croatian Exclusive Economic Zone",
    "Danish Exclusive Economic Zone",
    "Estonian Exclusive Economic Zone",
    "Finnish Exclusive Economic Zone",
    "French Exclusive Economic Zone",
    "Georgian Exclusive Economic Zone",
    "Greek Exclusive Economic Zone",
    "Icelandic Exclusive Economic Zone",
    "Irish Exclusive economic Zone",
    "Italian Exclusive Economic Zone",
    "Latvian Exclusive Economic Zone",
    "Lithuanian Exclusive Economic Zone",
    "Maltese Exclusive Economic Zone",
    "Norwegian Exclusive Economic Zone",
    "Polish Exclusive Economic Zone",
    "Portuguese Exclusive Economic Zone",
    "Romanian Exclusive economic Zone",
    "Russian Exclusive economic Zone",
    "Montenegrin Exclusive economic Zone",
    "Slovenian Exclusive Economic Zone",
    "Spanish Exclusive Economic Zone",
    "Swedish Exclusive Economic Zone",
    "Overlapping claim Ukrainian Exclusive Economic Zone",
    "United Kingdom Exclusive Economic Zone",
    "Turkish Exclusive Economic Zone",
    "Cocos Islands Exclusive Economic Zone",
    "Christmas Island Exclusive Economic Zone",
    "Norfolk Island Exclusive Economic Zone",
    "Australian Exclusive Economic Zone (Macquarie Island)",
    "New Caledonian Exclusive Economic Zone",
    "Vanuatu Exclusive Economic Zone",
    "Solomon Islands Exclusive Economic Zone",
    "Palau Exclusive Economic Zone",
    "Micronesian Exclusive Economic Zone",
    "Nauruan Exclusive Economic Zone",
    "Marshall Islands Exclusive Economic Zone",
    "Wake Island Exclusive Economic Zone",
    "Northern Marianes and Guam Exclusive Economic Zone",
    "Overlapping claim Taiwanese Exclusive Economic Zone",
    "Philippines Exclusive Economic Zone",
    "Australian Exclusive Economic Zone",
    "Papua New Guinean Exclusive Economic Zone",
    "Fijian Exclusive Economic Zone",
    "Tuvaluan Exclusive Economic Zone",
    "South Korean Exclusive Economic Zone",
    "North Korean Exclusive Economic Zone",
    "Overlapping claim Paracel Islands Exclusive Economic Zone: China / Taiwan / Vietnam",
    "Spratly Islands Exclusive Economic Zone",
    "Cambodian Exclusive Economic Zone",
    "Thailand Exclusive Economic Zone",
    "Indian Exclusive Economic Zone (Andaman and Nicobar Islands)",
    "Comoran Exclusive Economic Zone",
    "Mayotte Exclusive Economic Zone",
    "Glorioso Exclusive Economic Zone",
    "Seychellois Exclusive Economic Zone",
    "Réunion Exclusive Economic Zone",
    "Juan de Nova Exclusive Economic Zone",
    "Bassas da India Exclusive Economic Zone",
    "Ile Europa Exclusive Economic Zone",
    "Tromelin Exclusive Economic Zone",
    "Mauritian Exclusive Economic Zone",
    "Maldives Exclusive Economic Zone",
    "Sri Lankan Exclusive Economic Zone",
    "Mozambican Exclusive Economic Zone",
    "Madagascan Exclusive Economic Zone",
    "Kenyan Exclusive Economic Zone",
    "Somali Exclusive Economic Zone",
    "Eritrean Exclusive Economic Zone",
    "Djiboutian Exclusive Economic Zone",
    "Yemeni Exclusive Economic Zone",
    "Omani Exclusive Economic Zone",
    "Sudanese Exclusive Economic Zone",
    "Saudi Arabian Exclusive Economic Zone",
    "Kuwaiti Exclusive Economic Zone",
    "Bahraini Exclusive Economic Zone",
    "Pakistani Exclusive Economic Zone",
    "United Arab Emirates Exclusive Economic Zone",
    "Portuguese Exclusive Economic Zone (Azores)",                                
    "Cape Verdean Exclusive Economic Zone",                                          
    "Portuguese Exclusive Economic Zone (Madeira)",
    "Spanish Exclusive Economic Zone (Canary Islands)",
    "Overlapping claim Gibraltarian Exclusive Economic Zone",
    "Tunisian Exclusive Economic Zone",
    "Moroccan Exclusive Economic Zone",
    "Overlapping claim Western Saharan Exclusive Economic Zone",
    "Mauritanian Exclusive Economic Zone",
    "Gambian Exclusive Economic Zone",
    "Senegalese Exclusive Economic Zone",
    "Libyan Exclusive Economic Zone",
    "Syrian Exclusive Economic Zone",
    "Lebanese Exclusive Economic Zone",
    "Israeli Exclusive Economic Zone",
    "Cypriote Exclusive Economic Zone",
    "Algerian Exclusive Economic Zone",
    "Ascension Exclusive Economic Zone",
    "New Zealand Exclusive Economic Zone"]


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
        "entries": {
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

def getFishingEvents(month, year, country, collection):
    # set endDay to number of days in month
    endDay = 31
    # Set endDay to number of days in month
    if month == "02":
        endDay = 28
    elif month == "04" or month == "06" or month == "09" or month == "11":
        endDay = 30
    headers = {"Authorization": f"Bearer {apiKey}", "Content-Type": "application/json"}
    eez = codes[countries.index(country)]
    # Params for datasets, startDate, endDate, and country code (regions.eez)
    params = {
        "datasets": ["public-global-fishing-events:latest"],
        "startDate": str(year) + "-" + str(month) + "-01",
        "endDate": str(year) + "-" + str(month) + "-" + str(endDay),
        "region": {
            "dataset": "public-eez-areas",
            "id": codes[countries.index(country)]        
        }
    }
    # print(params)
    response = requests.post(gfwUrl, headers=headers, data=json.dumps(params))
    responseJson = None
    if response.status_code == 200 or response.status_code == 201:
        responseJson = response.json()
        # Upload each record in responseJson to mongo, using current year month country as key
        for record in responseJson["entries"]:
            collection.insert_one({"year": year, "month": month, "country": country, "record": record})
        return len(responseJson["entries"])
    else:
        print("Error:", response.json())
    # Print response
    # print(len(responseJson["entries"]))
    # print(responseJson)
    # return len(responseJson["entries"])
    return None

# Get fishing events for each month of 2022 for each 1 latitude by 1 longitude square on the globe
# client = pymongo.MongoClient("mongodb+srv://theresatvan:UEi8751OX1jaT9lz@cluster0.6jfc5iw.mongodb.net/")
# mydatabase = client["gfw"]
# mycollection = mydatabase["gfwDutchEEZ2020"]
# mycollection.delete_many({})
# Get fishing events for each month of 2022 for each 1 latitude by 1 longitude square on the globe
# f = open("output2.txt", "w")
# center = (41, -74) # NYC
# radius = 10
# Iterate through all lat/long squares in radius of center
# for lat in range(center[0]-radius, center[0]+radius):
#     for long in range(center[1]-radius, center[1]+radius):
#         # Get fishing events for each month
#         for month in months:
#             getFishingEvents(month, lat, long, mycollection)
            # print("starting lat: " + str(lat) + ", starting long: " + str(long) + ", month: " + str(month) + ", num events: " + str(getFishingEvents(month, lat, long)), file=f)
# f.close()

# Get fishing events for three small EEZs in OECD for 2010-2015
eezs = ["Belgian Exclusive Economic Zone", "Latvian Exclusive Economic Zone", "Estonian Exclusive Economic Zone"]
years = [2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020]
client = pymongo.MongoClient("mongodb+srv://theresatvan:UEi8751OX1jaT9lz@cluster0.6jfc5iw.mongodb.net/")
mydatabase = client["gfw"]
mycollection = mydatabase["gfw"]

# mycollection.delete_many({})

for eez in eezs:
    for year in years:
        for month in months:
            getFishingEvents(month, year, eez, mycollection)

client.close()



# Insert fishing events into Mongo

