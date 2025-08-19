from flask import Flask, request
import urllib.request, urllib.parse
import schedule
schema = "Logs"
number_of_docs = "1"
url ="http://54.210.181.24:31000/Schemas/" +schema +"/data?" + number_of_docs
print(url)

def get_data(url):
    request_url = urllib.request.Request(url)
    response = urllib.request.urlopen(request_url)
    print(response.read())

schedule.every(15).seconds.do(get_data, url)

while True:
    schedule.run_pending()

