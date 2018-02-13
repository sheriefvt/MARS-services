from mpipe import  Stage, Pipeline
from multiprocessing import freeze_support
import json
import ConfigParser,io
import wf_library


from bottle import get,error,  request, run

from pyparsing import Word, delimitedList, Optional, \
     alphas, alphanums, ParseException



with open ('mars.config', "r") as myfile:
        data=myfile.read()
config = ConfigParser.RawConfigParser(allow_no_value=True)
config.readfp(io.BytesIO(data))
server = config.get("MARS_configuration", "server")
host2 = config.get("MARS_configuration", "host2")
port2 = config.get("MARS_configuration", "port2")
port4 = config.get("MARS_configuration", "port4")
database_path = config.get("MARS_configuration", "database")
index_path1 = config.get("MARS_configuration", "index1")
index_path2 = config.get("MARS_configuration", "index2")


import sqlite3

def build_grammer():


    identifier = Word(alphas+"_", alphanums+"_")
    function_call = identifier.setResultsName("func",listAllMatches=True)

    wfstmt = Optional(delimitedList(function_call))

    return wfstmt


def parse_workflow(str):

   try:
        bSQL = build_grammer()
        tokens = bSQL.parseString( str,parseAll=True )
        return tokens

   except ParseException:
        return "exception"

class MyError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


def prepare_stages(tokens):
    stages = dict()
    m =0

    for t in tokens:

       stages[m]=Stage(eval('wf_library.{g}'.format(g=t)))
       m = m+1



    for i in range(0,len(stages)-1):
       stages[i].link(stages[i+1])


    return stages


@get('/workflowservice/library')
def do_query():
    db = sqlite3.connect(database_path)
    c = db.cursor()
    str = "select process_name,description from workflow_process"
    c.execute(str)
    data = c.fetchall()

    c.close()
    mylist = []
    for e in data:
         d={}
         d["process"]=e[0]
         d["description"]=e[1]
         mylist.append(d)

    return json.dumps(mylist)

def evaluate_workflow(stages,inp):
  # if __name__ == '__main__':
  #   freeze_support()
    pipe = Pipeline(stages[0])

    pipe.put(inp)

    pipe.put(None)

    return pipe.results()







@get('/workflowservice/execute')
def do_query():
 # if __name__ == '__main__':
  try:
     print 'request received'
     wf = request.query.get('wf')
     inp = request.query.get('input')


     t = parse_workflow(wf)


     p = prepare_stages(t)
     r=evaluate_workflow(p,inp)
     z = [a for a in r]

     return json.dumps({"datavalue":z})


  except error500 as e:
      return 'Error executing the workflow'


@error(500)
def error500(error):
    return 'Internal Error'


# if __name__ == '__main__':


run(server='cherrypy',host=host2, port=port4,debug=True)
