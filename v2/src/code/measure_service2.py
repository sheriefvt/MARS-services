__author__ = 'Sherif Abdelhamid'
#Measure Service Version 1.0 Beta

from bottle import get, post, request, run # or route
import os
import threading
import time

import networkx as nx
import sqlite3
import json
import datetime
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



class MyError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)



##End point to call the measure service to compute a specific measure on a network.
@get('/graphservice/measure/compute')
def do_compute():
    gname = request.query.get('graph')
    mid = request.query.get('measure')

    p = getmeasureinfo(mid)
    if p[3]=='None':
        par=''
    else:
        par = p[3]
    ts = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d--%H:%M:%S')
    if p[1]=="networkx":

        tmp=networkx_qsub(gname,p[0],ts,par)
    elif p[1] == "galib":
        tmp = galib_qsub(gname,p[2],ts,par,p[0])

    elif p[1] == "standalone":
        tmp = cplus_qsub(gname,p[2],ts,par,p[0])
    elif p[1] == "sql":
        tmp =sql_qsub(gname,ts,p[5],p[2],p[4],p[0])



    name =qsub_path +gname+"-"+mid+'.qsub'

    f1 = open(name, "w")
    f1.write(tmp)
    f1.close()
    if os.path.exists(name):
        qb = threading.Thread(name='qsub_worker', target=qsub_worker(name))
        qb.start()



    return

##Function to create qsub file for calculating degree within DBMS
def sql_qsub(gname,ts,dbname,sqlstmt,target,m):
    tmp2=sqlstmt.format(g=gname)

    tmp='''#!/bin/bash
#PBS -lwalltime=10:00:00
#PBS -W group_list=sipcinet
#PBS -q sfx_q
#PBS -N {gname}-{measure}-MARS
#PBS -o {qp}{graph_name}{ti}.qlog
cd $PBS_O_WORKDIR
sqlite3  {dp} 'update {g}_{tr} set {mn} = ({sq})'
    '''.format(g=gname,mn=dbname,ti=ts,tr=target,sq=tmp2,dp=database_path,measure=m,qp=qlog_path)

    return tmp

##Function to create qsub file for calculating different measures using networkx library. Currently, it calculates the degree,
# betweeness_centrality, clustering, load_centrality, node_clique_number, and closeness_centrality.

def networkx_qsub(gname,command,ts,parameter):
    tmp='''#!/bin/bash
#PBS -lwalltime=10:00:00
#PBS -W group_list=sipcinet
#PBS -q sfx_q
#PBS -N {graph_name}-{measure}-MARS
#PBS -o {qp}{graph_name}{ti}.qlog
cd $PBS_O_WORKDIR
export PATH=/home/sipcinet/edison/python-2.7.9/bin:$PATH
python {cp}measure.py {gp}{graph_name} {op}{graph_name}_{measure}.out {measure} {graph_name} {pr}
    '''.format(graph_name=gname,measure=command,ti=ts,cp=code_path,op=output_path,pr=parameter,gp=graph_path,qp=qlog_path)

    return tmp

##Function to create qsub file for calculating keshell using code provided by Chris Kulhman. Code is an executable.
def cplus_qsub(gname,mname,ts,parameter,command):
    tmp='''#!/bin/bash
#PBS -lwalltime=10:00:00
#PBS -W group_list=sipcinet
#PBS -q sfx_q
#PBS -N {graph_name}-{cmd}-MARS
#PBS -o {qp}{graph_name}{ti}.qlog
cd $PBS_O_WORKDIR
export PATH=/home/sipcinet/edison/python-2.7.9/bin:$PATH
{cp}{measure} {gp}{graph_name}.uel {pr} {op}{graph_name}_{cmd}.out
    '''.format(graph_name=gname,measure=mname,ti=ts,cp=code_path,gp=graph_path,pr=parameter,cmd=command,op=output_path,qp=qlog_path)

    return tmp

##Function to create qsub file for calculating clustering coef. using code provided by Maliq. Code is an executable
def galib_qsub(gname,mname,ts,parameter,cmd2):
    tmp='''#!/bin/sh

#PBS -l walltime=10:00:00
#PBS -l nodes=10:ppn=1
#PBS -W group_list=ndssl
#PBS -q ndssl_q
#PBS -A ndssl
#PBS -N {graph_name}-{cmd}-MARS
#PBS -o {qp}{graph_name}{ti}.qlog
#PBS -j oe

. /etc/profile.d/modules.sh


module add mvapich2/gcc
#module add mpiexec

time mpiexec -f $PBS_NODEFILE {cp}{command} {gp}{graph_name}.gph {op}{graph_name}_{cmd}.out {pr}
exit;
    '''.format(graph_name=gname,command=mname,cp=code_path,ti=ts,pr=parameter,cmd=cmd2,gp=graph_path,op=output_path,qp=qlog_path)

    return tmp



##Load measure information from DB
def getmeasureinfo(m):

    db = sqlite3.connect(database_path)
    c = db.cursor()
    sqlt = ("select id,package,command,parameter,target from measure where id='{c}'").format(c=m)
    c.execute(sqlt)
    data = c.fetchone()
    return data

##Submit qsub job request
def qsub_worker(name):

    os.system('qsub {filename}'.format(filename=name))





run(server=server,host=host, port=port3,debug=True)
