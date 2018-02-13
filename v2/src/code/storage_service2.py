from bottle import get, request, run, HTTPResponse

import sqlite3
import json
import ConfigParser,io
import testpopen


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

class MyError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


##End point to add new network to MARS
@get('/graphservice/storage/addnetwork')
def do_upload():
    name = request.query.get('name')
    if checkifphyicaltableexist(name,'node') and checkifphyicaltableexist(name,'edge'):
        return json.dumps({"error:network already exists"})
    else:
        uploadfile(name)
        return json.dumps({"state":"success"})

##Read network properties from .md file and stores in the DB
def gstoredb(g):


    with open (g, "r") as myfile:
        data=myfile.read()
    config = ConfigParser.RawConfigParser(allow_no_value=True)
    config.readfp(io.BytesIO(data))

    nname = config.get("Graph_Info", "name")
    nfilename = config.get("Graph_Info", "name")
    ndesc = config.get("Graph_Info", "description")
    ndirected = config.get("Graph_Info", "directed")
    nweighted = config.get("Graph_Info", "weighted")
    nlabeled = config.get("Graph_Info", "labeled")
    nnodes = config.get("Graph_Info", "nodes")
    nedges = config.get("Graph_Info", "edges")
    naddedby = config.get("Graph_Info", "addedby")
    nattributes = config.get("Graph_Info", "nodeattributes")
    eattributes = config.get("Graph_Info", "edgeattributes")
    ntype = config.get("Graph_Info",'originaltype')
    available  = config.get("Graph_Info",'available')
    details  = config.get("Graph_Info",'details')
    simple = config.get("Graph_Info",'simple')

    try:
        # connect to edison database
        db = sqlite3.connect(database_path)
        c1 = db.cursor()
        c2 = db.cursor()
        c3 = db.cursor()
        #insert graph info into the database
        str= "INSERT INTO Network (Name,File_Name,Description, Directed, Weighted, Labeled, Nodes, Edges ,Added_By,Original_type,Node_Attributes,Edge_Attributes,Available, details, simple) VALUES ('{name}','{fname}','{desc}','{directed}','{weighted}','{labeled}',{nodes},{edges},'{addedby}','{origin}','{natt}','{eatt}','{av}','{de}','{si}')".format(name=nname, fname=nfilename,desc=ndesc, directed=ndirected,weighted=nweighted, labeled=nlabeled, nodes=nnodes, edges=nedges,addedby=naddedby,origin=ntype ,natt=nattributes,eatt=eattributes,av=available,de=details,si=simple)

        c1.execute(str)

        if nattributes == "None":
          natt =''
        else:
          natt = nattributes.replace(';',',')
          natt = ","+natt
        nodetable = nname+"_node"

        str2 = "CREATE TABLE "+nodetable+ " (id integer"+natt+")"
        print str2
        if eattributes == 'None':
            eatt=''
        else:
            eatt = eattributes.replace(';',',')
            eatt = ","+eatt

        edgetable = nname+"_edge"
        str3 = "CREATE TABLE "+edgetable+ " (start integer, end integer "+eatt+")"
        print str3

        c2.execute(str2)
        c3.execute(str3)

        db.commit()
        db.close()

    except MyError as e:
        return "Error : msg".format(msg=e.value)


##End point for the Network Storage Service. Get graphs information
@get('/graphservice/storage/graph')
def do_graphs():
 try:
     user = request.query.get('user',default = 'no')
     db = sqlite3.connect(database_path)
     c = db.cursor()

     if user == 'no':
        c.execute("select network_id,Name,File_Name,Description, Directed, Weighted, Labeled, Nodes, Edges,Original_type,Node_Attributes,Edge_Attributes from network where Available = 'true'")

     else:
       c.execute("select network_id,Name,File_Name,Description, Directed, Weighted, Labeled, Nodes, Edges,Original_type,Node_Attributes,Edge_Attributes from network where Available = 'true' and Added_By = '{u}'".format(u=user))


     data = c.fetchall()
     c.close()
     mylist=[]
     for e in data:

        d={}

        d["graph_id"]=e[0]
        d["name"]=e[1]
        d["file_name"]=e[2]
        d["description"]=e[3]
        d["directed"]=e[4]
        d["weighted"]=e[5]
        d["labeled"]=e[6]
        d["numberOfNodes"]=e[7]
        d["numberOfEdges"]=e[8]
        d["original_format"]=e[9]

        temp = e[10].split(";")
        x={}

        for i in range(0,len(temp)):
           tt = temp[i].split(" ")
           x[tt[0]]=tt[1]

        d["node_attributes"]=x

        if e[11] != "false":
            temp2 = e[11].split(";")
            x2={}

            for i in range(0,len(temp2)):
                tt2 = temp2[i].split(" ")
                x2[tt2[0]]=tt2[1]

                d["edge_attributes"]=x2


        mylist.append(d)
     return json.dumps(mylist)

 except sqlite3.Error as e:
        return HTTPResponse(status=400, body="Database Error:" +e.args[0])

##End point for the Network Storage Service. Filter graphs based on attribute values.
@get('/graphservice/storage/graph/filter')
def do_graphs():
 try:
    attr = request.query.get('attribute')
    operator = request.query.get('operator')
    value =request.query.get('rvalue')
    db = sqlite3.connect(database_path)
    c = db.cursor()

    str = "select network_id,Name,File_Name,Description, Directed, Weighted, Labeled, Nodes, Edges,Original_type,Node_Attributes,Edge_Attributes from network where {att} {op} '{v}' and Available = 'true'".format(att=attr,op=operator,v=value)

    c.execute(str)

    data = c.fetchall()
    c.close()
    mylist=[]
    for e in data:

        d={}

        d["graph_id"]=e[0]
        d["name"]=e[1]
        d["file_name"]=e[2]
        d["description"]=e[3]
        d["directed"]=e[4]
        d["weighted"]=e[5]
        d["labeled"]=e[6]
        d["numberOfNodes"]=e[7]
        d["numberOfEdges"]=e[8]
        d["original_format"]=e[9]

        temp = e[10].split(";")
        x={}

        for i in range(0,len(temp)):
           tt = temp[i].split(" ")
           x[tt[0]]=tt[1]

        d["node_attributes"]=x

        if e[11] != "false":
            temp2 = e[11].split(";")
            x2={}

            for i in range(0,len(temp2)):
                tt2 = temp2[i].split(" ")
                x2[tt2[0]]=tt2[1]

                d["edge_attributes"]=x2


        mylist.append(d)
    return json.dumps(mylist)



 except sqlite3.Error as e:
        return HTTPResponse(status=400, body="Database Error: " +e.args[0])


##End point returns services directory
@get('/graphservice/directory')
def do_dir():

  return json.dumps([{"hostname":host2},
                    {"server":server},
                    {"portnumber":port},
                    {"index1_directory":index_path1},
                    {"index2_dorectory":index_path2},
                    {"qsub_directory":"/home/sipcinet/edison/graphservices/qsub/"},
                    {"database_directory":database_path}])


##End point for the Network Storage Service. Returns graph attributes.
@get('/graphservice/storage/graph/getattribute')
def do_getattribute():

        network = request.query.get('network')
        target = request.query.get('target')

        db = sqlite3.connect(database_path)
        c = db.cursor()
        str = "select {t}_attributes from network where name = '{n}'".format(n=network,t=target)
        print str
        c.execute(str)
        data = c.fetchone()
        x= data[0]
        y = x.split(";")
        att_list=[]
        for i in y:
            h = i.split(" ")
            att_list.append(h[0])
        c.close()
        return json.dumps({"datavalue":att_list})


##Helper function used by add network end pont in two steps.
def uploadfile(name):


    try:
      if not checkiftableexist(name):

         gstoredb(file_path+name+".md")
         testpopen.load_data(name)


      else:
         print "Error: Network already exist"


    except MyError as e:
        return "Error : msg".format(msg=e.value)


##End point used by measure service to notify storage service that a measure computation is completed.
##Storage service will check if measure already exist. If not, add measure to network tables and update network metadata
@get('/graphservice/storage/measure_notify')
def do_notify():
    network = request.query.get('network')
    measure = request.query.get('measure')
    print "Request received to add measure {m} for network {n}".format(m=measure,n=network)
    networkid = getnetworkid(network)
    nid = networkid[0]
    if checkmeasureexist(nid,measure):
        return json.dumps({'check':'Measure Already Exist.'})

    else:

        addnewcolumn(network,measure)
        readfiletable(network,measure)

        return json.dumps({'check':'Measure Added.'})

##Helper function used by measure_notify end point.
def readfiletable(network,measure):
    p=getmeasureinfo(measure)
    db = sqlite3.connect(database_path)

    if p[6] == "node":
        str = "CREATE TABLE {g} (id integer, {dn} {dt})".format(g=network+"_"+measure,dn=p[1],dt='text')
        c1=db.cursor()
        c1.execute(str)
        db.commit()

    else:
        str = "CREATE TABLE {g} (start integer, end integer, {dn} {dt})".format(g=network+"_"+measure,dn=p[2],dt='text')
        c1=db.cursor()
        c1.execute(str)
        db.commit()

    testpopen.load_data2(network+"_"+measure,measure)
    if p[6]=='node':
        str = "update {table} set {col} = (select {col} from {table2} where id = {table}.id)".format(table2=network+"_"+measure,table=network+"_"+p[6],col=p[1])

    else:
        str = "update {table} set {col} = (select {col} from {table2} where start = {table}.start and end = {table}.end)".format(table2=network+"_"+measure,table=network+"_"+p[6],col=p[1])
    c1=db.cursor()
    c1.execute(str)
    db.commit()

    str = "drop table {table2}".format(table2=network+"_"+measure)
    c1=db.cursor()
    c1.execute(str)
    db.commit()

    networkid = getnetworkid(network)
    nid = networkid[0]
    str = "insert into network_measure (Measure,Network) values ({m},{n})".format(n=nid,m=measure)
    c1=db.cursor()
    c1.execute(str)
    db.commit()

    str="select {name}_attributes from network where name= '{n}'".format(name=p[6],n=network)
    c1.execute(str)
    dt=c1.fetchone()

    if dt[0]=='None':
        tmp=""
        str="update network set {colt}_attributes = '{t}'||'{att}' where name='{n}'".format(n=network,colt=p[6],t=tmp,att=p[1]+" "+p[8])
    else:
        tmp=dt[0]
        str="update network set {colt}_attributes = '{t}'||';{att}' where name='{n}'".format(n=network,colt=p[6],t=tmp,att=p[1]+" "+p[8])

    c1=db.cursor()
    c1.execute(str)
    db.commit()

def addnewcolumn(network,measure):
    p = getmeasureinfo(measure)
    db = sqlite3.connect(database_path)
    if p[6] == 'node':
         tablename = network+"_node"


         c1 = db.cursor()
         str="alter table {name} add column {cname} {dtype}".format(name=tablename,cname=p[1],dtype=p[8])
         c1.execute(str)
         db.commit()

    else:
             tablename = network+"_edge"
             c1 = db.cursor()
             str="alter table {name} add column {cname} {dtype}".format(name=tablename,cname=p[1],dtype=p[8])
             c1.execute(str)
             db.commit()

    db.close()





def getmeasureinfo(m):

    db = sqlite3.connect(database_path)
    c = db.cursor()
    sqlt = ("select * from measure where id='{c}'").format(c=m)
    c.execute(sqlt)
    data = c.fetchone()
    return data

def getnetworkid(g):

    db = sqlite3.connect(database_path)
    c = db.cursor()
    sqlt = ("select network_id from network where name='{c}'").format(c=g)
    c.execute(sqlt)
    data = c.fetchone()
    return data







def checkiftableexist(name):
    db = sqlite3.connect(database_path)
    c1 = db.cursor()
    str="SELECT count(*) FROM network WHERE name='{name}'".format(name=name)
    c1.execute(str)
    data = c1.fetchone()
    db.close()
    if int(data[0])>0:
        return True
    else:
        return False

##Check if network already exists or not.
def checkifphyicaltableexist(name,target):
    db = sqlite3.connect(database_path)
    c1 = db.cursor()
    if target=='node':
        str="SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{name}_node'".format(name=name)
    else:
        str="SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{name}_edge'".format(name=name)
    c1.execute(str)
    data = c1.fetchone()
    db.close()
    if int(data[0])>0:
        return True
    else:
        return False

##Store in DB that this measure is calculated for this network.
def update_network_measure(network,measure):
        db = sqlite3.connect(database_path)
        str = "insert into network_measure (network,measure) values ({n},{m})".format(n=network,m=measure)
        c1 = db.cursor()
        c1.execute(str)
        db.commit()

        db.close()

##Check if measure already exist.
def checkmeasureexist(network,measure):

    db = sqlite3.connect(database_path)
    str = "select count(*) from network_measure where network = {n} and measure = {m}".format(n=network,m=measure)
    c1 = db.cursor()
    c1.execute(str)
    data = c1.fetchone()
    db.close()
    if int(data[0])>0:
        return True
    else:
        return False


run(server=server,host=host2, port=port)