__author__ = 'sherief'


import sqlite3
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

db = sqlite3.connect(database_path)

print 'Reseting sample network to initial state..'
str = "alter table sample_node rename to d_d"
c1=db.cursor()
c1.execute(str)
db.commit()

str = "create table sample_node as select id from d_d"
c1=db.cursor()
c1.execute(str)
db.commit()

str = "drop table d_d"
c1=db.cursor()
c1.execute(str)
db.commit()

str = "update network set node_attributes = 'id integer' where name = 'sample'"
c1=db.cursor()
c1.execute(str)
db.commit()

str = "delete from network_measure where network=87"
c1=db.cursor()
c1.execute(str)
db.commit()

print '\n\n\n\n'

print 'Reseting sample2 network to initial state..'
str = "alter table sample2_node rename to d_d"
c1=db.cursor()
c1.execute(str)
db.commit()

str = "create table sample2_node as select id from d_d"
c1=db.cursor()
c1.execute(str)
db.commit()

str = "drop table d_d"
c1=db.cursor()
c1.execute(str)
db.commit()

str = "update network set node_attributes = 'id integer' where name = 'sample2'"
c1=db.cursor()
c1.execute(str)
db.commit()

str = "delete from network_measure where network=111"
c1=db.cursor()
c1.execute(str)
db.commit()

