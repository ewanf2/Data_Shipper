import urllib.request, urllib.parse
from elasticsearch import Elasticsearch, helpers

import schedule
import json
import time
import os
from dotenv import load_dotenv

load_dotenv()
time_interval = os.getenv("time_interval")

schema = "fighters"  # os.getenv("SCHEMA")
number_of_docs = os.getenv("NUMBER_OF_DOCS")
index_name = "fighter-stats"
# ES_KEY = os.getenv("ES_KEY")
ES_URL = os.getenv("ES_URL")
DGEN_URL = os.getenv("DGEN_URL")
ES_PASSWORD = os.getenv("ES_PASSWORD")

api_url = DGEN_URL + "/Schemas/" + schema + "/data?no=" + number_of_docs
headers = {"Accept": "application/json"}
client = Elasticsearch(ES_URL, basic_auth=("elastic", ES_PASSWORD), verify_certs=False, ssl_show_warn=False)

schema_spec = {"fighters": {
    "Name": {"type": "name"},
    "sex": {"type": "sex", "parameters": {"a": 1, "b": 1}},
    "style": {"type": "style"},
    "weightclass": {"type": "weightclass"},
    "height": {
        "type": "gauss int",
        "dependencies": {
            "categorical": ["weightclass"],
            "Flyweight": {"mu": 160, "sigma": 5},
            "Bantamweight": {"mu": 165, "sigma": 5},
            "Featherweight": {"mu": 170, "sigma": 5},
            "Lightweight": {"mu": 175, "sigma": 5},
            "Welterweight": {"mu": 180, "sigma": 5},
            "Middleweight": {"mu": 185, "sigma": 5},
            "Light Heavyweight": {"mu": 190, "sigma": 5},
            "Heavyweight": {"mu": 195, "sigma": 5}
        }
    }, "reach": {
        "type": "gauss int",
        "dependencies": {
            "categorical": ["weightclass"],
            "Flyweight": {"mu": 160, "sigma": 5},
            "Bantamweight": {"mu": 165, "sigma": 5},
            "Featherweight": {"mu": 170, "sigma": 5},
            "Lightweight": {"mu": 175, "sigma": 5},
            "Welterweight": {"mu": 180, "sigma": 5},
            "Middleweight": {"mu": 185, "sigma": 5},
            "Light Heavyweight": {"mu": 190, "sigma": 5},
            "Heavyweight": {"mu": 195, "sigma": 5}
        }
    },

    "KOs": {
        "type": "gauss int",
        "dependencies": {
            "categorical": ["style", "weightclass"],
            "boxer": {
                "Flyweight": {"mu": 6, "sigma": 7},
                "Bantamweight": {"mu": 8, "sigma": 7},
                "Featherweight": {"mu": 10, "sigma": 7},
                "Lightweight": {"mu": 11, "sigma": 7},
                "Welterweight": {"mu": 12, "sigma": 7},
                "Middleweight": {"mu": 13, "sigma": 7},
                "Light Heavyweight": {"mu": 14, "sigma": 7},
                "Heavyweight": {"mu": 15, "sigma": 7}
            },
            "wrestler": {
                "Flyweight": {"mu": 1, "sigma": 5},
                "Bantamweight": {"mu": 1, "sigma": 5},
                "Featherweight": {"mu": 2, "sigma": 5},
                "Lightweight": {"mu": 2, "sigma": 5},
                "Welterweight": {"mu": 3, "sigma": 5},
                "Middleweight": {"mu": 4, "sigma": 5},
                "Light Heavyweight": {"mu": 5, "sigma": 5},
                "Heavyweight": {"mu": 6, "sigma": 5}
            },
            "muay thai": {
                "Flyweight": {"mu": 5, "sigma": 6},
                "Bantamweight": {"mu": 6, "sigma": 6},
                "Featherweight": {"mu": 7, "sigma": 6},
                "Lightweight": {"mu": 8, "sigma": 6},
                "Welterweight": {"mu": 9, "sigma": 6},
                "Middleweight": {"mu": 10, "sigma": 6},
                "Light Heavyweight": {"mu": 11, "sigma": 6},
                "Heavyweight": {"mu": 12, "sigma": 6}
            },
            "jiu-jitsu": {
                "Flyweight": {"mu": 1, "sigma": 4},
                "Bantamweight": {"mu": 1, "sigma": 4},
                "Featherweight": {"mu": 2, "sigma": 4},
                "Lightweight": {"mu": 3, "sigma": 4},
                "Welterweight": {"mu": 3, "sigma": 4},
                "Middleweight": {"mu": 4, "sigma": 4},
                "Light Heavyweight": {"mu": 5, "sigma": 4},
                "Heavyweight": {"mu": 5, "sigma": 4}
            },
            "judo": {
                "Flyweight": {"mu": 2, "sigma": 4},
                "Bantamweight": {"mu": 2, "sigma": 4},
                "Featherweight": {"mu": 3, "sigma": 4},
                "Lightweight": {"mu": 4, "sigma": 4},
                "Welterweight": {"mu": 5, "sigma": 4},
                "Middleweight": {"mu": 5, "sigma": 4},
                "Light Heavyweight": {"mu": 6, "sigma": 4},
                "Heavyweight": {"mu": 6, "sigma": 4}
            },
            "karate": {
                "Flyweight": {"mu": 4, "sigma": 5},
                "Bantamweight": {"mu": 5, "sigma": 5},
                "Featherweight": {"mu": 6, "sigma": 5},
                "Lightweight": {"mu": 7, "sigma": 5},
                "Welterweight": {"mu": 9, "sigma": 5},
                "Middleweight": {"mu": 10, "sigma": 5},
                "Light Heavyweight": {"mu": 11, "sigma": 5},
                "Heavyweight": {"mu": 12, "sigma": 5}
            },
            "kickboxing": {
                "Flyweight": {"mu": 8, "sigma": 7},
                "Bantamweight": {"mu": 9, "sigma": 7},
                "Featherweight": {"mu": 10, "sigma": 7},
                "Lightweight": {"mu": 11, "sigma": 7},
                "Welterweight": {"mu": 12, "sigma": 7},
                "Middleweight": {"mu": 13, "sigma": 7},
                "Light Heavyweight": {"mu": 14, "sigma": 7},
                "Heavyweight": {"mu": 16, "sigma": 7}
            }
        }
    },
    "Submissions": {
        "type": "gauss int",
        "dependencies": {
            "categorical": ["style", "weightclass"],
            "boxer": {
                "Flyweight": {"mu": 2, "sigma": 2},
                "Bantamweight": {"mu": 2, "sigma": 2},
                "Featherweight": {"mu": 3, "sigma": 2},
                "Lightweight": {"mu": 3, "sigma": 2},
                "Welterweight": {"mu": 2, "sigma": 2},
                "Middleweight": {"mu": 2, "sigma": 2},
                "Light Heavyweight": {"mu": 1, "sigma": 2},
                "Heavyweight": {"mu": 1, "sigma": 2}
            },
            "wrestler": {
                "Flyweight": {"mu": 8, "sigma": 3},
                "Bantamweight": {"mu": 9, "sigma": 3},
                "Featherweight": {"mu": 9, "sigma": 3},
                "Lightweight": {"mu": 10, "sigma": 3},
                "Welterweight": {"mu": 8, "sigma": 3},
                "Middleweight": {"mu": 7, "sigma": 3},
                "Light Heavyweight": {"mu": 6, "sigma": 3},
                "Heavyweight": {"mu": 5, "sigma": 3}
            },
            "muay thai": {
                "Flyweight": {"mu": 3, "sigma": 2},
                "Bantamweight": {"mu": 3, "sigma": 2},
                "Featherweight": {"mu": 3, "sigma": 2},
                "Lightweight": {"mu": 2, "sigma": 2},
                "Welterweight": {"mu": 2, "sigma": 2},
                "Middleweight": {"mu": 2, "sigma": 2},
                "Light Heavyweight": {"mu": 1, "sigma": 2},
                "Heavyweight": {"mu": 1, "sigma": 2}
            },
            "jiu-jitsu": {
                "Flyweight": {"mu": 15, "sigma": 4},
                "Bantamweight": {"mu": 16, "sigma": 4},
                "Featherweight": {"mu": 16, "sigma": 4},
                "Lightweight": {"mu": 17, "sigma": 4},
                "Welterweight": {"mu": 15, "sigma": 4},
                "Middleweight": {"mu": 14, "sigma": 4},
                "Light Heavyweight": {"mu": 13, "sigma": 4},
                "Heavyweight": {"mu": 12, "sigma": 4}
            },
            "judo": {
                "Flyweight": {"mu": 12, "sigma": 4},
                "Bantamweight": {"mu": 13, "sigma": 4},
                "Featherweight": {"mu": 13, "sigma": 4},
                "Lightweight": {"mu": 14, "sigma": 4},
                "Welterweight": {"mu": 12, "sigma": 4},
                "Middleweight": {"mu": 11, "sigma": 4},
                "Light Heavyweight": {"mu": 10, "sigma": 4},
                "Heavyweight": {"mu": 9, "sigma": 4}
            },
            "karate": {
                "Flyweight": {"mu": 4, "sigma": 2},
                "Bantamweight": {"mu": 4, "sigma": 2},
                "Featherweight": {"mu": 4, "sigma": 2},
                "Lightweight": {"mu": 3, "sigma": 2},
                "Welterweight": {"mu": 3, "sigma": 2},
                "Middleweight": {"mu": 2, "sigma": 2},
                "Light Heavyweight": {"mu": 2, "sigma": 2},
                "Heavyweight": {"mu": 2, "sigma": 2}
            },
            "kickboxing": {
                "Flyweight": {"mu": 2, "sigma": 2},
                "Bantamweight": {"mu": 2, "sigma": 2},
                "Featherweight": {"mu": 2, "sigma": 2},
                "Lightweight": {"mu": 2, "sigma": 2},
                "Welterweight": {"mu": 1, "sigma": 2},
                "Middleweight": {"mu": 1, "sigma": 2},
                "Light Heavyweight": {"mu": 1, "sigma": 2},
                "Heavyweight": {"mu": 1, "sigma": 2}
            }
        }
    }, "Wins": {"type": "gauss int", "dependencies": {"numerical": ["KOs", "Submissions"],
                                                      "formula": "(KOs) + (Submissions) +gauss_int(5,5)"}},
    "Draw": {"type": "gauss int", "parameters": {"mu": 0, "sigma": 3}},
    "Loss": {"type": "gauss int", "parameters": {"mu": 3, "sigma": 6}},
    "Country": {"type": "country code"},
    "Stance": {"type": "stance"},
    "Avg_fight_time": {
        "type": "clamped gauss",
        "parameters": {"mu": 13, "sigma": 3, "max": 25}
    },
    "Strike_accuracy": {
        "type": "gauss int",
        "dependencies": {
            "categorical": ["style"],
            "boxer": {"mu": 75, "sigma": 5},
            "kickboxing": {"mu": 70, "sigma": 5},
            "muay thai": {"mu": 65, "sigma": 5},
            "jiu-jitsu": {"mu": 50, "sigma": 10},
            "wrestler": {"mu": 45, "sigma": 10},
            "karate": {"mu": 60, "sigma": 7},
            "judo": {"mu": 50, "sigma": 7}
        }
    },
    "Strikes_landed": {
        "type": "gauss int",
        "dependencies": {
            "numerical": ["Avg_fight_time", "Strike_accuracy"],
            "formula": "int(Avg_fight_time * random.uniform(3, 6) * Strike_accuracy / 100)"
        }
    },

    "Organisation":{"type": "Org"},
    "Takedowns_landed": {
        "type": "gauss int",
        "dependencies": {
            "categorical": ["style"],
            "boxer": {"mu": 1, "sigma": 1},
            "kickboxing": {"mu": 1, "sigma": 1},
            "muay thai": {"mu": 2, "sigma": 2},
            "jiu-jitsu": {"mu": 4, "sigma": 3},
            "wrestler": {"mu": 5, "sigma": 3},
            "karate": {"mu": 1, "sigma": 1},
            "judo": {"mu": 3, "sigma": 2}
        }
    }
}
}

schema = list(schema_spec.keys())[0]


def get_data(url, headers):
    request_url = urllib.request.Request(url, headers=headers, method='GET')
    with urllib.request.urlopen(request_url) as response:
        data = response.read().decode('utf-8')
    return data


def create_index(index_name):
    if not client.indices.exists(index=index_name):
        client.indices.create(index=index_name)
        print(f"Index created: {index_name}")
    else:
        print(f"Index already exists: {index_name}")


def send_data(url, headers):
    data = get_data(url, headers=headers)
    if not client.indices.exists(index=index_name):
        client.indices.create(index=index_name)
    data = json.loads(data)
    actions = []
    for i in data:
        action = {"_index": index_name,
                  "_source": i}
        actions.append(action)
    helpers.bulk(client, actions)

    print("Data sent to elasticsearch:" + str(actions))


# print(schema_spec)

def create_schema(schema):  # creating schema if it doesn't exist already
    url = DGEN_URL + "/Schemas"  # view all schemas api endpoint
    request_url = urllib.request.Request(url, method='GET')
    with urllib.request.urlopen(request_url) as response:
        data = response.read().decode('utf-8')

    schema_list = json.loads(data)
    if schema not in schema_list:
        request_url = urllib.request.Request(url, method='POST', headers={
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }, data=json.dumps(schema_spec).encode('utf-8'))

        with urllib.request.urlopen(request_url) as response:
            data = response.read().decode('utf-8')
        print("Schema created")
    else:
        print("Schema already exists")


def main():
    create_schema(schema)  # create schema and index initially
    create_index(index_name)
    send_data(api_url, headers)  # send an initial batch of data
    print(f"Sending data to {ES_URL} every {time_interval} seconds...")
    schedule.every(int(time_interval)).seconds.do(send_data, api_url, headers)
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
