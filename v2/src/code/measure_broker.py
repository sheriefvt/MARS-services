__author__ = 'Sherif Abdelhamid'

import os,time
import requests
import signal

url = "http://edisondev.vbi.vt.edu:9001/graphservice/storage/measure_notify"

import ConfigParser,io


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

def notify_storage_service(g,m):
    data = {'network':g,'measure':m}
    r=requests.get(url,params=data)
    print r.text

joblist=list()

def signal_handler(signal, frame):
    global interrupted
    interrupted = True

signal.signal(signal.SIGINT, signal_handler)

try:
 interrupted = False
 while True:
        qs = os.popen('qstat')

        lines = qs.readlines()
        print joblist
        for line in lines[2:]:
            pid,job,user,ts,status,queue = line.split()
            tmp = job.split("-")
            if status=='C'  and "-MARS" in job and os.path.isfile(output_path+tmp[0]+"_"+tmp[1]+".out") and not(job in joblist) :


                     print "new job completed:"+job
                     joblist.append(job)
                     if notify=="True":
                        notify_storage_service(tmp[0],tmp[1])



        time.sleep(3)
        if interrupted:
            print '\nclear list..'
            del joblist[:]
            print("Gotta go..")

            exit()
except KeyboardInterrupt:

    pass




