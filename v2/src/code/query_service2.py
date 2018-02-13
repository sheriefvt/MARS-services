from bottle import get,error, post, request, run, HTTPResponse,route, static_file

import sqlite3
import json
import threading
import time
import dicttoxml
import ConfigParser,io

import sql3

from whoosh.index import create_in
from whoosh.fields import *
from whoosh.qparser import QueryParser
from whoosh import index

from multiprocessing import  Queue
output = Queue()



with open ('mars.config', "r") as myfile:
        data=myfile.read()
config = ConfigParser.RawConfigParser(allow_no_value=True)
config.readfp(io.BytesIO(data))
server = config.get("MARS_configuration", "server")
host = config.get("MARS_configuration", "host")
host2 = config.get("MARS_configuration", "host2")
port = config.get("MARS_configuration", "port")
port2 = config.get("MARS_configuration", "port2")
port3 = config.get("MARS_configuration", "port3")
database_path = config.get("MARS_configuration", "database")
index_path1 = config.get("MARS_configuration", "index1")
index_path2 = config.get("MARS_configuration", "index2")
file_path = config.get("MARS_configuration", "uploadfile")
workflow_path = config.get("MARS_configuration","workflow")



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
     session = request.query.get('session')
     nruns = request.query.get('nruns',default=0)
     details = request.query.get('details',default=True)
     memo = request.query.get('memo',default=False)
     csvf = request.query.get('csvf',default=False)


     [tokens,msg] = sql3.parse_query(content,graph,nruns,view,session)
     start = time.time()

     if tokens =='exception':
        return json.dumps({'check':'Invalid'})


     if msg == 'fail':
        return json.dumps({'check':'Invalid'})


     if msg == 'ok' and validate=='True':

        return json.dumps({'check':'Valid'})

     if ( sql3.check_query_exist(content,details,1) and view=='property' and memo=='True') :
        print 'memoization'
        [rc,sss]=  sql3.get_query_result(content,view)


        if rc == 'err':
            return json.dumps({'check':'Invalid'})

        else:
            if validate =='True':
             return json.dumps({'check':'Valid'})
            else:
             qid = sql3.check_query_exist(content,details,2)
             temp = {"qid":qid,"result":json.loads(rc)}

             return temp



     elif msg == 'ok' and validate=='False':

        [sql_final,mode,simple] = sql3.transform(tokens,graph,session,details)

        if view == 'property' and simple !='yes':
            return json.dumps({'check':'Invalid'})

        if view == 'seed' and simple == 'yes':
            return json.dumps({'check':'Invalid'})

        if simple == 'yes':

            temp = sql3.execute_sql(sql_final,mode,graph)
            jsonresult={mode+"_sets":[temp]}

        elif simple=='no':

            squeries = sql_final.split(";")



            myDict = [sql3.execute_sql(q,mode,graph) for q in squeries]

            jsonresult={mode+"_sets":myDict}

        else:

            r =int("".join(tokens.runs))

            if parallel == 'False':
                #sequential execution
                print 'sequential mode'

                myDict = [sql3.execute_sql(sql_final,mode,graph) for i in range(0,r)]

                jsonresult={mode+"_sets":myDict}

            else:

                #parallel execution
                print 'parallel mode'

                jobs = []
                for i in range(0,r):
                    dg = threading.Thread(name='worker', target=callwebservice,args=(sql_final,mode,graph))
                    jobs.append(dg)
                    dg.start()

                for proc in jobs:
                    proc.join()
                        #following a queue/message pump model
                xtime = time.time()-start
                print 'total time without dict creating = {to}'.format(to=xtime)
                myDict = [output.get() for c in jobs]


                jsonresult={mode+"_sets":myDict}
        fresult = json.dumps(jsonresult)

        qid=sql3.store_query(content,graph,fresult,mode,simple,details)
        if qid == 'exist':
            qid = sql3.check_query_exist(content,details,2)

        if csvf == 'True':


          xml = dicttoxml.dicttoxml(json.loads(fresult))
          fq = {'qid':qid,'result':xml}

        else:
          fq = {'qid':qid,'result':jsonresult}

        ttime = time.time()-start
        print 'total time = {to}'.format(to=ttime)

        return json.dumps(fq)


     elif msg=='try':
         #return HTTPResponse(status=400, body="Do you mean "+tokens)

          return json.dumps({'check':"Do you mean "+tokens})

     else:
        print 'd'
        return json.dumps({'check':'Invalid'})




  except error500 as e:
      return 'Error executing the query'


@error(500)
def error500(error):
    return 'Internal Error'

##Convert json to CSV. Currently, not used, but can be used in the future.
def json2csv(j,mode,view,header):
    jj = json.loads(j)
    if view =="property":

            n= jj['node_sets'][0][mode]
            ck = ""
            for k in n[0]:
                ck = ck+ k+","
            ckn = ck[:-1]
            cc = ""
            for i in n:

                c = ""

                for g  in i:
                    c = c + str(i[g]) +","
                cc = cc +c[:-1]+"\n"
            if header ==True:
                return json.dumps(ckn+"\n"+cc)
            else:
                return json.dumps(cc)


##End point for performing set operations on query results. Query results are stored as sets.
@get('/graphservice/setoperation/save')
def do_save():
    sname = request.query.get('setname')
    qid = request.query.get('queryid')
    sid = request.query.get('sessionid')
    if not check_session(sid):
        return json.dumps({'check':'Invalid'})
    tid = get_set_type(qid)
    db = sqlite3.connect(database_path)
    c = db.cursor()
    str = "insert into result_set (query,setname,session,type)  values ({c},'{g}',{s},'{t}')".format(c=qid,g=sname,s=sid,t=tid)
    print  str
    c.execute(str)

    db.commit()
    lid = c.lastrowid
    return json.dumps(lid)

##Check query set type [node or edge]
def get_set_type(id):

    db = sqlite3.connect(database_path)
    c = db.cursor()
    sqlt = ("select target from query_repository where query_id ={d}").format(d=id)
    c.execute(sqlt)
    data = c.fetchone()

    return data[0]

##Check if set already exists
def check_session(id):

    db = sqlite3.connect(database_path)
    c = db.cursor()
    sqlt = ("select count(*) from session where id ={d} and state='started'").format(d=id)
    c.execute(sqlt)
    data = c.fetchone()
    if data[0]>0:
        return True
    else:
        return False


##End point to delete set by id
@get('/graphservice/setoperation/delete')
def do_save():
    seid = request.query.get('setid')
    db = sqlite3.connect(database_path)
    c = db.cursor()
    str = "delete from result_set where  id  = {c}".format(c=seid)

    c.execute(str)

    db.commit()

##End point to download files
@route('/graphservice/download/<filename:path>')
def download(filename):
    return static_file(filename, root=workflow_path, download=filename)

##End point to create a session
@get('/graphservice/session/create')
def do_create():
    aid = request.query.get('appid')

    db = sqlite3.connect(database_path)
    c = db.cursor()
    str = "insert into session (application,state)  values ({c},'{g}')".format(c=aid,g='started')

    c.execute(str)

    db.commit()
    lid = c.lastrowid
    return json.dumps(lid)

##End point to end an existing session
@get('/graphservice/session/end')
def do_create():
    sid = request.query.get('sessionid')
    aid = request.query.get('appid')
    db = sqlite3.connect(database_path)
    c = db.cursor()
    str = "update session set state = 'ended' where id = {c} and application= {d}".format(c=sid,d=aid)

    c.execute(str)

    c = db.cursor()
    str = "delete from result_set where session  = {d}".format(d=sid)

    c.execute(str)

    db.commit()
    c.close()

##Create recursive calls to query service
def callwebservice(stmt,mode,graph):

    r=sql3.execute_sql(stmt,mode,graph)
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

    resultset={}

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





##End point to show query repository statistics on usage. Can be used throw an admin dashboard.
@get('/graphservice/visualization')
def do_viz():
    mode = request.query.get('mode')
    if mode=="qtype":
        db = sqlite3.connect(database_path)
        c = db.cursor()
        str = "select count(*) from query_repository where view = 'seed'"
        c.execute(str)
        data = c.fetchone()
        x= data[0]
        c = db.cursor()
        str = "select count(*) from query_repository where view = 'property'"
        c.execute(str)
        data2 = c.fetchone()
        y= data2[0]
        c.close()
        return json.dumps([{"type":"sample","number":"{t}".format(t=x)},{"type":"property","number":"{t}".format(t=y)}])

    elif mode =="ntype":
        mylist=[]

        db = sqlite3.connect(database_path)
        c = db.cursor()
        str = "select network_name,count(*) from query_repository group by network_name"
        c.execute(str)
        data = c.fetchall()
        print data
        c.close()
        for e in data:
             d={}
             d["network"]=e[0]
             d["number"]=e[1]
             mylist.append(d)

        return json.dumps(mylist)

    elif mode =="ttype":
        mylist=[]
        db = sqlite3.connect(database_path)
        c = db.cursor()
        str = "select target,count(*) from query_repository group by target"
        c.execute(str)
        data = c.fetchall()
        print data
        c.close()
        for e in data:
             d={}
             d["type"]=e[0]
             d["number"]=e[1]
             mylist.append(d)

        return json.dumps(mylist)

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



run(server=server,host=host2, port=port2,debug=True)



