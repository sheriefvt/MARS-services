__author__ = 'Sherif'


import sys
import subprocess


graph_path = '/home/sipcinet/edison/graphservices/graphs/'
database_path = '/home/sipcinet/edison/graphservices/database/Edison.db'
output_path='/home/sipcinet/edison/graphservices/output/'


def load_data(name,nodefile,edgefile):

    args1 = ["sqlite3", "-separator", " ", database_path, ".import {gp}{gname} {g}_node".format(g=name,gname=nodefile,gp=graph_path)]
    args2 = ["sqlite3", "-separator", " ", database_path, ".import {gp}{gname} {g}_edge".format(g=name,gname=edgefile,gp=graph_path)]



    subprocess.call(args1)
    subprocess.call(args2)

name = sys.argv[1]
nodefile = sys.argv[2]
edgefile = sys.argv[3]


print 'load data for network'

load_data(name,nodefile,edgefile)

print 'Finished loading data'
