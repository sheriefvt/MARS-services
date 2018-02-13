
import requests
import ConfigParser,io

with open ('mars.config', "r") as myfile:
        data=myfile.read()
config = ConfigParser.RawConfigParser(allow_no_value=True)
config.readfp(io.BytesIO(data))
host2 = config.get("MARS_configuration", "host2")
port2 = config.get("MARS_configuration", "port2")

urlin = 'http://'+host2+":"+port2+"/graphservice/search/createindex"

datain1={'view':'seed'}
datain2={'view':'property'}

r1 = requests.get(urlin,params=datain1)
r2 = requests.get(urlin,params=datain2)
