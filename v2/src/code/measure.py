__author__ = 'Sherif Abdelhamid'

#! /usr/bin/python

#

import networkx as nx
import sys

import sqlite3
import time

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
qlog_path = config.get("MARS_configuration", "qlog")



def getmeasureinfo(m):

    db = sqlite3.connect(database_path)
    c = db.cursor()
    sqlt = ("select command,target from measure where id='{c}'").format(c=m)
    c.execute(sqlt)
    data = c.fetchone()
    return data



def networkxcal(graph,command,target,output):
    print 'Step1: Reading network {g}'.format(g=graph)
    start = time.time()
    G = nx.read_edgelist(graph+'.uel')

    saveout = sys.stdout

    sys.stdout = open(output, 'w')

    if target=='node':
        for v in G:

          exec('print str({id})+" "+str({cm})'.format(id=v,cm=command))

    else:
        for edge in G.edges_iter(data=True):

          exec('print str({u})+" "+str({v})+" "+str({b})'.format(u=edge[0],v=edge[1],b=command))


    sys.stdout = saveout
    t=time.time()-start

    return t

def getids(graph):
    db = sqlite3.connect(database_path)
    c = db.cursor()
    sqlt = ("select network_id from network where name='{c}'").format(c=graph)
    print sqlt
    c.execute(sqlt)
    data = c.fetchone()
    return data[0]





d=getids(sys.argv[4])


com = getmeasureinfo(sys.argv[3])




t=networkxcal(sys.argv[1],com[0],com[1],sys.argv[2])












