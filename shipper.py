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
schema = os.getenv("SCHEMA")
number_of_docs = os.getenv("NUMBER_OF_DOCS")
index_name = "logs"
ES_KEY = os.getenv("ES_KEY")
ES_URL = os.getenv("ES_URL")
DGEN_URL = os.getenv("DGEN_URL")
ES_PASSWORD = os.getenv("ES_PASSWORD")

api_url =DGEN_URL+"/Schemas/" +schema +"/data?no=" + number_of_docs
headers = {"Accept":"application/json"}
client = Elasticsearch(ES_URL,api_key=ES_KEY,verify_certs=False,ssl_show_warn=False)




def get_data(url,headers):
    request_url = urllib.request.Request(url, headers=headers,method='GET')
    with urllib.request.urlopen(request_url) as response:
        data = response.read().decode('utf-8')
    return data


def send_data(url,headers):
    data = get_data(url,headers=headers)
    if not client.indices.exists(index=index_name):
        client.indices.create(index=index_name)
    data=json.loads(data)
    actions = []
    for i in data:
        action = {"_index": index_name,
                  "_source": i}
        actions.append(action)
    helpers.bulk(client,actions)


    print("Data sent to elasticsearch:" + str(actions))







schedule.every(3).seconds.do(send_data, api_url,headers)

while True:
    schedule.run_pending()



