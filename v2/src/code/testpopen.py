__author__ = 'sherief'


import subprocess
import ConfigParser,io


with open ('mars.config', "r") as myfile:
        data=myfile.read()
config = ConfigParser.RawConfigParser(allow_no_value=True)
config.readfp(io.BytesIO(data))
server = config.get("MARS_configuration", "server")
host2 = config.get("MARS_configuration", "host2")
port = config.get("MARS_configuration", "port")
port2 = config.get("MARS_configuration", "port2")
port3 = config.get("MARS_configuration", "port3")
database_path = config.get("MARS_configuration", "database")
index_path1 = config.get("MARS_configuration", "index1")
index_path2 = config.get("MARS_configuration", "index2")
file_path = config.get("MARS_configuration", "uploadfile")
output_path = config.get("MARS_configuration", "output")

def load_data(name):

    args1 = ["sqlite3", "-separator", " ", database_path, ".import {gp}{gname}.nodes {gname}_node".format(gname=name,gp=file_path)]

    args2 = ["sqlite3", "-separator", " ", database_path, ".import {gp}{gname}.uel {gname}_edge".format(gname=name,gp=file_path)]



    subprocess.call(args1, shell=True)
    subprocess.call(args2,shell=True)


def load_data2(name,measure):
    if measure=='7':
        args1 = ["sqlite3", "-separator", "\t\t", database_path, ".import {op}{gname}.out {gname}".format(gname=name,op=output_path)]
    else:
        args1 = ["sqlite3", "-separator", " ", database_path, ".import {op}{gname}.out {gname}".format(gname=name,op=output_path)]

    subprocess.call(args1)

