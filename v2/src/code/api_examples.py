__author__ = 'sherief'

import requests

import ConfigParser,io,time

with open ('mars.config', "r") as myfile:
        data=myfile.read()
config = ConfigParser.RawConfigParser(allow_no_value=True)
config.readfp(io.BytesIO(data))
host2 = config.get("MARS_configuration", "host2")
port2 = config.get("MARS_configuration", "port2")

urlprod2 = 'http://'+host2+":"+port2+"/graphservice/search"


data3 = {'keywords': "sample",'metadata':'content','view':'property'}
r2 = requests.get(urlprod2,params=data3)

print r2.text