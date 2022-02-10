import requests
import json

data = requests.get( "https://w101gqjv56.execute-api.ap-south-1.amazonaws.com/test/s3-json-data")
data=json.loads(data.text)
# print(tuple(data["body"][0].keys()))
list1 = []
for i in data["body"]:
    list1.append(tuple(i.values()))

print(list1)