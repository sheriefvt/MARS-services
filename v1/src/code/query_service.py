from bottle import get,error,  request, run, HTTPResponse # or route

import sqlite3
import json
import threading
import sys

import sql_prod
from whoosh.index import create_in

from whoosh.fields import *
from whoosh.qparser import QueryParser
from whoosh import index

from multiprocessing import  Queue
output = Queue()
import ConfigParser,io


with open ('mars.config', "r") as myfile:
        data=myfile.read()
config = ConfigParser.RawConfigParser(allow_no_value=True)
config.readfp(io.BytesIO(data))
server = config.get("MARS_configuration", "server")
host = config.get("MARS_configuration", "host")
port = config.get("MARS_configuration", "port")
database_path = config.get("MARS_configuration", "database")
index_path1 = config.get("MARS_configuration", "index1")
index_path2 = config.get("MARS_configuration", "index2")


class MyError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

##End point for query Service
@get('/graphservice/query')
def do_query():

  try:
     content = request.query.get('content')
     graph = request.query.get('graph')
     validate = request.query.get('validate',default=False)
     parallel = request.query.get('parallel',default=False)
     view = request.query.get('view')
     nruns = request.query.get('nruns',default=0)

     [tokens,msg] = sql_prod.parse_query(content,graph,nruns,view)

     if tokens =='exception':

        return json.dumps({'check':'Invalid'})
         

     if msg == 'fail':

        return json.dumps({'check':'Invalid'})
        

     if msg == 'ok' and validate=='True':

        return json.dumps({'check':'Valid'})

     if ( sql_prod.check_query_exist(content) and view=='property') :
        rc =  sql_prod.get_query_result(content,view)

        if rc == 'err':

            return json.dumps({'check':'Invalid'})
            		
        else:
            if validate =='True':
             return json.dumps({'check':'Valid'})
            else:
             return rc

     

     elif msg == 'ok' and validate=='False':

        [sql_final,mode,simple] = sql_prod.transform(tokens,graph)

        if view == 'property' and simple !='yes':
            return json.dumps({'check':'Invalid'})

        if view == 'seed' and simple == 'yes':
            return json.dumps({'check':'Invalid'})

        if simple == 'yes':

            temp = sql_prod.execute_sql(sql_final,mode,graph)
            jsonresult={mode+"_sets":[temp]}

        elif simple=='no':

            squeries = sql_final.split(";")


            myDict = [sql_prod.execute_sql(q,mode,graph) for q in squeries]

            jsonresult={mode+"_sets":myDict}
            
        else:

            r =int("".join(tokens.runs))
            sql_final = sql_final[1:]
            squeries = sql_final.split(",")


            if parallel == 'False':
                #sequential execution
                print 'sequential mode'

                myDict = [sql_prod.execute_sql(squeries[i],mode,graph)  for i in range(0,r)]

                jsonresult={mode+"_sets":myDict}

            else:

                #parallel execution
                print 'parallel mode'
                sql_final = sql_final[1:]
                squeries = sql_final.split(",")
                jobs = []
                for i in range(0,r):

                    dg = threading.Thread(name='worker', target=callwebservice,args=(squeries[i],mode,graph))
                    jobs.append(dg)
                    dg.start()

                for proc in jobs:
                    proc.join()
                        #following a queue/message pump model

                myDict = [output.get() for c in jobs]


                jsonresult={mode+"_sets":myDict}

        fresult = json.dumps(jsonresult)
        sql_prod.store_query(content,graph,fresult,mode,simple)

        return fresult


     elif msg=='try':

          
          return json.dumps({'check':"Do you mean "+tokens})   

     else:
        print 'd'
        return json.dumps({'check':'Invalid'})




  except error500 as e:
      return 'Error executing the query'


@error(500)
def error500(error):
    return 'Internal Error'


def callwebservice(stmt,mode,graph):

    r=sql_prod.execute_sql(stmt,mode,graph)
    output.put(r)


##End point for Query Search Service
@get('/graphservice/search')
def do_search():
 try:
    key = request.query.get('keywords')
    view = request.query.get('view')
    meta = request.query.get('metadata',default='content')
    if view =='property':
     ix = index.open_dir(index_path1)
    elif view =='seed':
     ix = index.open_dir(index_path2)
    query = QueryParser(meta, schema=ix.schema)

    q = query.parse(key,)
    
    with ix.searcher() as s:
        results = s.search(q, groupedby="target")
        
        mylist=[]
        d={}
        for hit in results:
            
            d["query_id"]=hit["query_id"]
            d["content"]=hit["content"]
            d["graph"]=hit["network_name"]
            d["results"]=hit["results"]
            d["target"]=hit["target"]
            mylist.append(d.copy())
    return json.dumps(mylist)
 except error500 as e:
     return

##End point for deleting index
@get('/graphservice/search/deleteindex')
def do_delete():
    id = request.query.get('id')
    view = request.query.get('view')
    if view =='property':
     ix = index.open_dir(index_path1)
    elif view =='seed':
     ix = index.open_dir(index_path2)

    ix.delete_by_term('query_id', id)


##End point to create an index
@get('/graphservice/search/createindex')
def do_index():
    view = request.query.get('view')
    ##These are the columns to be indexed. Note: it is better to have the names consistent with the attribute names in the DB.
    ##The indexes are the query_id, content, etc.
    schema = Schema(query_id=TEXT(stored=True),content=TEXT(stored=True),network_name=TEXT(stored=True),results=TEXT(stored=True),target=TEXT(stored=True))

    if view =='property':
         ix1 = create_in(index_path1,schema)
         print 'Index1 created at '+index_path1
    elif view =='seed':
         ix2 = create_in(index_path2,schema)
         print 'Index2 created at '+index_path2



##End point to view query repository
@get('/graphservice/repository')
def do_repository():
    type = request.query.get('type')
    view = request.query.get('view')

    if view =='property':
     ix = index.open_dir(index_path1)
     
    elif view =='seed':
     ix = index.open_dir(index_path2)

    
    query = QueryParser('target', schema=ix.schema)



    if type.lower()=='node':
      q = query.parse('node',)
      with ix.searcher() as s:
        results = s.search(q,limit=None)
        
        mylist=[]
        d={}
        for hit in results:
         d["query_id"]=hit["query_id"]
         d["content"]=hit["content"]
         d["graph"]=hit["network_name"]
         d["results"]=hit["results"]
         d["target"]=hit["target"]
         mylist.append(d.copy())

         resultset = {"node_queries":mylist}
        return json.dumps(resultset)

    elif type.lower()=='edge':
      q = query.parse('edge',)
      with ix.searcher() as s:
        results = s.search(q,limit=None)

        mylist=[]
        d={}
        for hit in results:
         d["query_id"]=hit["query_id"]
         d["content"]=hit["content"]
         d["graph"]=hit["network_name"]
         d["results"]=hit["results"]
         d["target"]=hit["target"]
         mylist.append(d.copy())
         resultset = {"edge_queries":mylist}
        return json.dumps(resultset)

    elif type.lower()=='all':
      q = query.parse('node',)
      with ix.searcher() as s:
        results = s.search(q,limit=None)

        mylist1=[]
        d={}
        for hit in results:
         d["query_id"]=hit["query_id"]
         d["content"]=hit["content"]
         d["graph"]=hit["network_name"]
         d["results"]=hit["results"]
         d["target"]=hit["target"]
         mylist1.append(d.copy())


      q = query.parse('edge',)
      with ix.searcher() as s:
        results = s.search(q,limit=None)

        mylist2=[]
        d={}
        for hit in results:
         d["query_id"]=hit["query_id"]
         d["content"]=hit["content"]
         d["graph"]=hit["network_name"]
         d["results"]=hit["results"]
         d["target"]=hit["target"]
         mylist2.append(d.copy())


        resultset={}
        resultset["node_queries"]=mylist1
        resultset["edge_queries"]=mylist2

        return json.dumps(resultset)
    else:
        return HTTPResponse(status=400, body="Invalid Input")




##End point for the Network Storage Service. Get graphs information
@get('/graphservice/storage/graph')
def do_graphs():
 try:
     db = sqlite3.connect(database_path)
     c = db.cursor()

     c.execute("select network_id,Name,File_Name,Description, Directed, Weighted, Labeled, Nodes, Edges,Original_type,Node_Attributes,Edge_Attributes from network where Available = 'true'")

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
    str = "select network_id,Name,File_Name,Description, Directed, Weighted, Labeled, Nodes, Edges,Original_type,Node_Attributes,Edge_Attributes from network where "+attr+" "+operator+" "+value+" and  Available = 'true'"
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

  return json.dumps([{"hostname":host},
                    {"server":server},
                    {"portnumber":port},
                    {"index1_directory":index_path1},
                    {"index2_dorectory":index_path2},
                    {"qsub_directory":"/home/sipcinet/edison/graphservices/qsub/"},
                    {"database_directory":database_path}])




@get('/graphservice/model')
def do_model():
    return json.dumps([{"model_name":"Progressive 2-state model","model_id":11, "sub_model_id":1, "description":"Threshold model where nodes may transition from state 0 to 1, but not from 1 to 0. The model supports blocking nodes: nodes that do not change state.",
     "parameters":[{"model":"integer", "sub_model":"integer","threshold":"integer", "type":"int_node_trait"},{"state":"integer", "is_fixed":"integer", "type":"int_node_state"},
                  ]},
                      {"model_name":"Back-and-forth 2-state model","model_id":11, "sub_model_id":3, "description":"Threshold model where nodes may transition from state 0 to 1, and from 1 to 0. The model supports blocking nodes: nodes that do not change state.",
     "parameters":[{"model":"integer", "sub_model":"integer","up_threshold":"integer","down_threshold":"integer", "type":"int_node_trait"},{"state":"integer", "is_fixed":"integer", "type":"int_node_state"},
                   ]},
                       {"model_name":"Back-and-forth 2-state model with influence from distance-2 neighbors","model_id":11, "sub_model_id":4, "description":"Threshold model where nodes may transition from state 0 to 1, and from 1 to 0. Neighboring nodes at distance 1 and distance 2 can influence a node.",
     "parameters":[{"model":"integer", "sub_model":"integer","up_threshold":"integer","down_threshold":"integer", "type":"int_node_trait"},{"state":"integer", "is_fixed":"integer", "type":"int_node_state"},
                   ]},
                       {"model_name":"SIR epidemic model","model id":22, "sub_model_id":1, "description":"Classic susceptible-infected-recovered epidemiological model.",
     "parameters":[{"model":"integer", "sub_model":"integer","duration_in_state_I":"integer", "type":"int_node_trait"},{"state":"integer", "type":"int_node_state"},{"edge_weight":"double", "type":"double_edge_state"}
                   ]},
                        {"model_name":"SIR epidemic model","model_id":22, "sub_model_id":3, "description":"Classic susceptible-infected-recovered epidemiological model, but a susceptible node may require multiple infecting neighbors to become infected.",
     "parameters":[{"model":"integer", "sub_model":"integer","duration_in_state I":"integer", "type":"int_node_trait"},{"state":"integer","threshold":"integer", "type":"int_node_state"},{"edge weight":"double", "type":"double_edge_state"}
                   ]},
                         {"model_name":"Linear Threshold model","model_id":37, "sub_model_id":0, "description":"This is the Linear Threshold model of Kempe et. al (KDD 2003).",
     "parameters":[{"model":"integer", "sub_model":"integer","type":"int_node_trait"},{"state":"integer", "type":"int_node_state"},{"threshold":"double", "type":"double_node_state"},{"edge influence":"double", "type":"double_edge_state"}
                   ]},
                         {"model_name":"Connected Components Threshold Model","model id":38, "sub_model_id":0, "description":"This is an influence model that uses thresholds, but now each set of neighbors of a node that form a connected component collectively influence the node (Ugander, PNAS 2012).",
     "parameters":[{"model":"integer", "sub_model":"integer","type":"int_node_trait"},{"state":"integer","is_fixed":"integer","up_threshold":"integer","down_threshold":"integer", "type":"int_node_state"}
                   ]}

                      ])




run(server=server,host=host, port=port,debug=True)
