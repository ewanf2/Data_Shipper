
import urllib.request, urllib.parse
from elasticsearch import Elasticsearch, helpers

import schedule
import json
from faker import Faker
import os
from dotenv import load_dotenv

load_dotenv()
time_interval = 2
fake = Faker()
schema = "fighters_new" #os.getenv("SCHEMA")
number_of_docs = os.getenv("NUMBER_OF_DOCS")
index_name = "fighter-stats"
ES_KEY = os.getenv("ES_KEY")
ES_URL = os.getenv("ES_URL")
DGEN_URL = os.getenv("DGEN_URL")
ES_PASSWORD = os.getenv("ES_PASSWORD")

api_url =DGEN_URL+"/Schemas/" +schema +"/data?no=" + number_of_docs
headers = {"Accept":"application/json"}
client = Elasticsearch(ES_URL,basic_auth=("elastic",ES_PASSWORD),verify_certs=False,ssl_show_warn=False)

schema_spec = {"fighters_new":{"Name": {"type": "name"},
                 "sex": {"type": "sex", "parameters": {"a": 1, "b": 1}},
                 "fightstyle": {"type": "style small"},
                 "weightclass": {"type": "Sizes"},
                 "KO wins": {"type": "gauss int",
                             "dependencies": {"categorical": ["fightstyle","weightclass" ],
                                              "boxer": {
                                                  "Flyweight": {"mu": 10, "sigma": 5},
                                                  "Lightweight": {"mu": 15, "sigma": 5},
                                                  "Heavyweight": {"mu": 25, "sigma": 5}
                                              },
                                              "wrestler": {
                                                  "Flyweight": {"mu": 1, "sigma": 5},
                                                  "Lightweight": {"mu": 3, "sigma": 5},
                                                  "Heavyweight": {"mu": 8, "sigma": 5}
                                              }

                                              }}

                               }

}
schema = list(schema_spec.keys())[0]
mapping = {"properties": {
    "Name":{"type": "keyword"},
    "sex":{"type": "keyword"},
    "fightstyle":{"type": "keyword"},
    "weightclass":{"type": "keyword"},
    "KO rate":{"type": "long"},

}}

def get_data(url,headers):
    request_url = urllib.request.Request(url, headers=headers,method='GET')
    with urllib.request.urlopen(request_url) as response:
        data = response.read().decode('utf-8')
    return data

def create_index(index_name):
    if not client.indices.exists(index=index_name):
        client.indices.create(index=index_name,mappings=mapping)
        print(f"Index created: {index_name}")
    else:
        print(f"Index already exists: {index_name}")

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

print(schema_spec)

def create_schema(schema): #creating schema if it doesn't exist already
    url = DGEN_URL + "/Schemas" #view all schemas api endpoint
    request_url = urllib.request.Request(url,method='GET')
    with urllib.request.urlopen(request_url) as response:
        data = response.read().decode('utf-8')

    schema_list = json.loads(data)
    if schema not in schema_list:
        request_url = urllib.request.Request(url,method='POST',headers={
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }, data= json.dumps(schema_spec).encode('utf-8'))

        with urllib.request.urlopen(request_url) as response:
            data = response.read().decode('utf-8')
        print("Schema created")
    else:
        print("Schema already exists")




def main():
    create_schema(schema) #create schema and index initially
    create_index(index_name)
    send_data(api_url,headers) #send an initial batch of data
    schedule.every(time_interval).minutes.do(send_data, api_url,headers)
    while True:
        schedule.run_pending()




if __name__ == "__main__":
    main()
