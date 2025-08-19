import random
import urllib.request, urllib.parse
from elasticsearch import Elasticsearch, helpers
import random
import schedule
import json
from faker import Faker
schema = "Logs"
number_of_docs = "10"
url ="http://54.210.181.24:31000/Schemas/" +schema +"/data?no=" + number_of_docs
headers = {"Accept":"application/json"}
client = Elasticsearch("https://54.210.181.24:9200",api_key="SWxaM3dwZ0JOWUhzMHA1RXQ3aE06TzBBd2RJSkE4bGpwVG84RjdLeHhUdw==",verify_certs=False)


def get_data(url,headers):
    request_url = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request_url) as response:
        data = response.read().decode('utf-8')
    return data


def send_data(url,headers):
    data = get_data(url,headers=headers)
    index =schema.lower()
    if not client.indices.exists(index=index):
        client.indices.create(index=index)
    data=json.loads(data)
    for i in data:
        item = json.dumps(i)
        client.index(index=index, id=str(random.randint(0,10000)), body=item)




    print("Data sent to elasticsearch:" + json.dumps(data))





schedule.every(10).seconds.do(send_data, url,headers)

while True:
    schedule.run_pending()



