
import requests
import ConfigParser,io

with open ('mars.config', "r") as myfile:
        data=myfile.read()
config = ConfigParser.RawConfigParser(allow_no_value=True)
config.readfp(io.BytesIO(data))
host = config.get("MARS_configuration", "host")
port = config.get("MARS_configuration", "port")

urlin = host+":"+port+"/graphservice/search/createindex"

datain1={'view':'seed'}
datain2={'view':'property'}

r1 = requests.get(urlin,params=datain1)
r2 = requests.get(urlin,params=datain2)
