import filecmp,requests,time

import ConfigParser,io

print 'Start verification testing for workflow service'

with open ('mars.config', "r") as myfile:
        data=myfile.read()
config = ConfigParser.RawConfigParser(allow_no_value=True)
config.readfp(io.BytesIO(data))
server = config.get("MARS_configuration", "server")
host2 = config.get("MARS_configuration", "host2")
port = config.get("MARS_configuration", "port")
port2 = config.get("MARS_configuration", "port2")
port4 = config.get("MARS_configuration", "port4")
database_path = config.get("MARS_configuration", "database")
index_path1 = config.get("MARS_configuration", "index1")
index_path2 = config.get("MARS_configuration", "index2")
file_path = config.get("MARS_configuration", "uploadfile")
qsub_path = config.get("MARS_configuration", "qsub")
graph_path = config.get("MARS_configuration", "graph")
code_path = config.get("MARS_configuration", "code")
output_path = config.get("MARS_configuration", "output")
notify = config.get("MARS_configuration", "storage_notify")


url ='http://'+host2+":"+port4+"/workflowservice/execute"

print "Executing workflow 1"
datawf = {'wf':"start,network_data,max_data,end",'input':"degree,sample,node,no condition"}

r = requests.get(url,params=datawf)

if r.text=='{"datavalue": [2]}':
    print "workflow results are valid"
else:
    print "workflow results are invalid"

print "Executing workflow 2"
datawf = {'wf':"start,network_data,min_data,end",'input':"degree,sample,node,no condition"}
r = requests.get(url,params=datawf)
if r.text=='{"datavalue": [1]}':
    print "workflow results are valid"
else:
    print "workflow results are invalid"


print "Executing workflow 3"
datawf = {'wf':"start,network_data,count_data,end",'input':"id,sample,node,no condition"}
r = requests.get(url,params=datawf)
if r.text=='{"datavalue": [6]}':
    print "workflow results are valid"
else:
    print "workflow results are invalid"


print "Executing workflow 4"
datawf= {'wf':"start,execute_query,query_data,end",'input':"select nodes from sample where degree = 1,sample,node|id"}


r = requests.get(url,params=datawf)

if r.text=='{"datavalue": [[5, 6]]}':
    print "workflow results are valid"
else:
    print "workflow results are invalid"


print "Executing workflow 5"
datawf= {'wf':"start,execute_query,query_data,sum_data,end",'input':"select nodes from sample where degree = 1,sample,node|clustering"}


r = requests.get(url,params=datawf)
if r.text=='{"datavalue": [0.0]}':
    print "workflow results are valid"
else:
    print "workflow results are invalid"


