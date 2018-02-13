import filecmp,requests,time

import ConfigParser,io

print 'Start verification testing for network measure service'

with open ('mars.config', "r") as myfile:
        data=myfile.read()
config = ConfigParser.RawConfigParser(allow_no_value=True)
config.readfp(io.BytesIO(data))
server = config.get("MARS_configuration", "server")
host = config.get("MARS_configuration", "host")
port = config.get("MARS_configuration", "port")
port2 = config.get("MARS_configuration", "port2")
port3 = config.get("MARS_configuration", "port3")
database_path = config.get("MARS_configuration", "database")
index_path1 = config.get("MARS_configuration", "index1")
index_path2 = config.get("MARS_configuration", "index2")
file_path = config.get("MARS_configuration", "uploadfile")
qsub_path = config.get("MARS_configuration", "qsub")
graph_path = config.get("MARS_configuration", "graph")
code_path = config.get("MARS_configuration", "code")
output_path = config.get("MARS_configuration", "output")
notify = config.get("MARS_configuration", "storage_notify")

print 'Testing measure service with network sample'

num_measure=9
url11 ='http://'+host+":"+port3+"/graphservice/measure/compute"

for l in range(1,num_measure):
    print 'requesting measure {i} for network sample'.format(i=l)
    data11={'graph':'sample','measure':l}
    r = requests.get(url11,params=data11)
    time.sleep(3)



print 'Waiting for files to be generated..'
time.sleep(5)

print 'Diffing files..'

for l in range(1 , num_measure):
    if filecmp.cmp(output_path+'sample_{i}.out'.format(i=l), output_path+'sample_{i}.out.valid'.format(i=l)):
        print 'Measure {i} valid'.format(i=l)
    else:
        print 'Measure {i} invalid'.format(i=l)


print 'Testing measure service with network sample2'


for l in range(1,num_measure):
    print 'requesting measure {i} for network sample2'.format(i=l)
    data11={'graph':'sample2','measure':l}
    r = requests.get(url11,params=data11)
    time.sleep(3)



print 'Waiting for files to be generated..'
time.sleep(5)

print 'Diffing files..'

for l in range(1 , num_measure):
    if filecmp.cmp(output_path+'sample2_{i}.out'.format(i=l), output_path+'sample2_{i}.out.valid'.format(i=l)):
        print 'Measure {i} valid'.format(i=l)
    else:
        print 'Measure {i} invalid'.format(i=l)


print 'network measure service validation test complete..'