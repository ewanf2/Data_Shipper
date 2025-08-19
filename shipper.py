import random
import urllib.request, urllib.parse
from elasticsearch import Elasticsearch, helpers
import random
import schedule
import json
from faker import Faker
import os
from dotenv import load_dotenv
load_dotenv()

fake = Faker()
schema = "Logs"
number_of_docs = "20"

ES_KEY = os.getenv("ES_KEY")
ES_URL = os.getenv("ES_URL")
DGEN_URL = os.getenv("DGEN_URL")
ES_PASSWORD = os.getenv("ES_PASSWORD")

api_url ="/Schemas/" +schema +"/data?no=" + number_of_docs
headers = {"Accept":"application/json"}
client = Elasticsearch(ES_URL,api_key=ES_KEY,verify_certs=False,ssl_show_warn=False)


def get_data(url,headers):
    request_url = urllib.request.Request(url, headers=headers,method='GET')
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





schedule.every(60).seconds.do(send_data, api_url,headers)

#while True:
 #   schedule.run_pending()



