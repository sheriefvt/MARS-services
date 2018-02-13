from pyparsing import Literal, CaselessLiteral, Word, delimitedList, Optional, \
    Combine, Group, alphas, nums, alphanums, ParseException, Forward, oneOf, quotedString, \
    ZeroOrMore, restOfLine, Keyword

import sqlite3,json,random
from whoosh import index
import ConfigParser,io

from string import whitespace

class MyError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


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
query_path = config.get("MARS_configuration", "query")

# define SQL-like language

def build_grammer():

    selectStmt = Forward()
    compoundselectStmt = Forward()
    subgraphselectStmt = Forward()
    sampleselectStmt = Forward()
    setStmt = Forward()
    selectToken  = oneOf("select find get what search list", caseless=True)
    fromToken   = Keyword("from", caseless=True)
    whereToken   = Keyword("where", caseless=True)
    sampleToken =Keyword("sample",caseless=True)
    subgraphToken =Keyword("subgraph",caseless=True)
    neighborsToken =Keyword("neighbors",caseless=True)
    targetToken = oneOf("edges nodes  node edge",caseless=True)
    ident          = Word( alphas, alphanums + "_$").setName("identifier")

    columnName = Combine((oneOf("v u e")+"."+ ident)) | Combine(neighborsToken+"("+Word(nums).setResultsName("friends",listAllMatches=True)+")"+"."+ident) |ident


    whereExpression = Forward()
    runs_ = Keyword("number",caseless=True)
    and_ = Keyword("and", caseless=True)
    or_ = Keyword("or", caseless=True)
    in_ = Keyword("in", caseless=True)
    size_ = Keyword("size", caseless=True)
    identifier = Word(alphas+"_", alphanums+"_")


    E = CaselessLiteral("E")
    binop = oneOf("= != < > >= <= eq ne lt le gt ge", caseless=True)


    setop = oneOf("union intersect except", caseless=True)
    arithSign = Word("+-",exact=1)
    realNum = Combine( Optional(arithSign) + ( Word( nums ) + "." + Optional( Word(nums) )  |
                                           ( "." + Word(nums) ) ) +
                   Optional( E + Optional(arithSign) + Word(nums) ) )




    samplestmt = (sampleToken+"("+runs_+"="+Word( nums ).setResultsName("runs")+","+size_+"="+"["+Word( nums ).setResultsName("lb")+","+Word( nums ).setResultsName("sample")+"]"+")")

    subgraphstmt = (subgraphToken.setResultsName("type")+"("+Word(nums).setResultsName("startnode")+","+Word(nums).setResultsName("depth")+")")

    intNum = Combine( Optional(arithSign) + Word( nums ) +
                  Optional( E + Optional("+") + Word(nums) ) )

    columnRval = realNum | intNum | quotedString | columnName.setResultsName("column",listAllMatches=True)



    whereCondition = (columnName.setResultsName("column",listAllMatches=True)+ binop + columnRval )|  ( columnName.setResultsName("column",listAllMatches=True) + in_ + "(" + columnRval +ZeroOrMore("," + columnRval)  + ")" )

    whereCondition2 = Group((columnName.setResultsName("column",listAllMatches=True)+ binop + columnRval )|  ( columnName.setResultsName("column",listAllMatches=True) + in_ + "(" + columnRval +ZeroOrMore("," + columnRval)  + ")" ) | ( "(" + whereExpression + ")" ))

    whereExpression << whereCondition + ZeroOrMore( ( and_ | or_ ) + whereExpression )


    defstmt =ident.setResultsName( "graph")+"."+ targetToken.setResultsName( "type") + "="+  "{" + delimitedList( whereCondition2 ).setResultsName("compactwhere") + "}"

    function_call = identifier.setResultsName("func",listAllMatches=True) + "(" + ((delimitedList(identifier|Word(nums)).setResultsName("args",listAllMatches=True))) + ")" | identifier.setResultsName("func",listAllMatches=True)

    wfstmt = Optional(delimitedList(function_call))

    selectStmt      << ( selectToken +
                     targetToken.setResultsName( "type" ) +
                     fromToken +
                     (ident.setResultsName( "graph"))+
                     Optional(whereToken + (whereExpression).setResultsName("where", listAllMatches=True) ))



    sampleselectStmt  << ( selectToken +samplestmt+
                     targetToken.setResultsName( "type") +
                     fromToken +
                     (ident.setResultsName( "graph"))+
                     Optional(whereToken + (whereExpression).setResultsName("where", listAllMatches=True) ))



    subgraphselectStmt  << ( selectToken +subgraphstmt +
                     fromToken +
                     (ident.setResultsName( "graph")))



    compoundselectStmt << selectStmt.setResultsName("select",listAllMatches=True) + ZeroOrMore(setop.setResultsName("setop",listAllMatches=True)  + selectStmt )

    setStmt << ident.setResultsName("setname",listAllMatches=True) + ZeroOrMore (setop.setResultsName("setp",listAllMatches=True) + ident.setResultsName("setname"))


    SQL = sampleselectStmt|compoundselectStmt|subgraphselectStmt|setStmt


    bSQL = SQL
    SqlComment = "--" + restOfLine
    bSQL.ignore( SqlComment )

    return bSQL


##Build grammer for SQL-like language.

bSQL = build_grammer()


##Define the parsing service. Currently, we parse sql-like queries only. Can be extended in the future.
def parse_query( str,graph,runs,view,session):

    try:

        tokens = bSQL.parseString(str,parseAll=True )



        if len(tokens.setname) > 0 and len(tokens.setp)>0:
               if validate_set2(tokens,session):
                   if validate_set(tokens,session) :
                      return tokens,"ok"
                   else:
                      return tokens,"fail"
               else:
                   return tokens,"fail"


        elif is_validgraph(tokens,graph)  and is_validview(tokens,runs,view):
             if len(tokens.column)>0:

                rr =is_validattr(tokens,graph)
                if rr[0]:

                    return rr[1],"ok"

                else:
                    if rr[1]=='fail':

                        return tokens,"fail"

                    else:

                        return rr[1],"try"

             else:
                 return tokens,"ok"

        else:

            return tokens,"fail"

    except ParseException, err:
        return "exception","err"

##Query Validation
def validate_set(tokens,session):

    l=[]
    for e in tokens.setname:
          l.append(get_set_type(e,session))

    if len(l) > len(set(l)):
       return True
    else:
       return False

##Query Validation
def validate_set2(tokens,session):

    l=[]
    for e in tokens.setname:
      if get_set_count(e,session) ==0:
       return False

    return True


def get_set_count(s,id):

    db = sqlite3.connect(database_path)
    c = db.cursor()
    sqlt = ("select count(*) from result_set where setname='{c}' and session={d}").format(c=s,d=id)
    c.execute(sqlt)
    data = c.fetchone()

    return data[0]


def get_set_type(s,id):

    db = sqlite3.connect(database_path)
    c = db.cursor()
    sqlt = ("select type from result_set where setname='{c}' and session={d}").format(c=s,d=id)
    c.execute(sqlt)
    data = c.fetchone()

    return data[0]

##Query Validation
def is_validruns(tokens,nruns):
    if len(tokens.runs)>0:

        if tokens.runs==nruns and int(tokens.runs) > 0:
            return True

        else:
            return False

    return True

##Query Validation
def is_validview(tokens,runs,view):
    if len(tokens.sample)>0 :
        if view =='seed':
            if is_lbvalid(tokens) and is_validruns(tokens,runs):
                return True
        else:
            return False
    else:
        if view =='property':
            return True
        else:
            return False

##Query Validation
def is_lbvalid(tokens):
    if len(tokens.lb)>0:
        if tokens.lb > tokens.sample :
            return False
        else:
             return True


##Query Validation
def is_validgraph(tokens,graph):
    dd = tokens.graph

    passed = True


    if graph != dd:
        passed=False


    return passed

##Query Validation
def is_validattr(tokens,graph):
    dd = tokens.column

    if tokens.type == 'node' or tokens.type == 'nodes':
     cc =getcolnames('node',graph)

    else:
     cc =getcolnames('edge',graph)


    passed = True


    for i in tokens.column:
      if (i[:2]=='v.' or i[:2]=='u.'  ):
        if tokens.type == 'node' or tokens.type == 'nodes':
           passed=False

           return passed,'fail'

        else:
            cc =getcolnames('node',graph)
            z=calcmind(cc[1],i[2:])

            if z[0]==-1:
                passed=False

                return passed,z[1]
            elif z[0]==0:
                passed=False

                return passed,'fail'


      elif (i[:2]=='e.'):

          if tokens.type == 'edge' or tokens.type == 'edges':
           passed=False

           return passed,'fail'

          else:
            cc =getcolnames('edge',graph)
            z=calcmind(cc[1],i[2:])

            if z[0]==-1:
                passed=False

                return passed,z[1]
            elif z[0]==0:
                passed=False

                return passed,'fail'

      else:
        z=calcmind(cc[1],i)


        if z[0]==-1:
            passed=False

            return passed,z[1]
        elif z[0]==0:
            passed=False

            return passed,'fail'

        elif z[0]==2:
            passed = False
            return passed,"fail"


    return passed,tokens

##Query Translation to SQL
def transform(tokens,graph,session,details):

    mode = 'node'
    simple = 'yes'
    newcontent =''
    dd = tokens.type
    dd2 = tokens.setop
    dd3 = tokens.compactwhere
    dd4 = tokens.sample
    dd5 = tokens.runs
    dd6 = tokens.column
    dd7 = tokens.setp


    if 'nodes' in dd or 'node' in dd :

        mode = 'node'

        if len(dd2)==0:

            if len(dd3)==0:
                if len(dd4)==0:
                    if any("e." in s for s in dd6):
                        newcontent = create_mixed_select2(tokens,graph,dd6,details)
                    else:
                        newcontent = create_simple_select(tokens,graph,'node',details)



                else:

                 if len(dd5)==0:
                  newcontent = create_sample_select(tokens,graph,'node',details)
                 else:
                  simple = 'run'
                  newcontent = create_sample_select(tokens,graph,'node',details)
            else:

                newcontent = create_compact_select(tokens,graph,'node',details)
                simple='no'
        else:


            newcontent =create_set_select(tokens,graph,'node',details)
    elif 'edges' in dd or 'edge' in dd :
        mode = 'edge'

        if len(dd2)==0:

            if len(dd3)==0:
                if len(dd4)==0:
                    if any("v." in s for s in dd6) or any("u." in s for s in dd6):


                        newcontent = create_mixed_select(tokens,graph,dd6,details)

                    else:

                        newcontent = create_simple_select(tokens,graph,'edge',details)


                else:
                 if len(dd5)==0:
                  newcontent = create_sample_select(tokens,graph,'edge',details)
                 else:
                  simple = 'run'
                  newcontent = create_sample_select(tokens,graph,'edge',details)
            else:

                newcontent = create_compact_select(tokens,graph,'edge',details)
                simple='no'
        else:


            newcontent =create_set_select(tokens,graph,'edge',details)

    elif 'subgraph' in dd :
        mode = 'node'
        newcontent =create_subgraph_select(tokens,graph,details)

    elif len(dd7)>0:
       mode = 'node'
       newcontent =create_setop_select(tokens,graph,session,details)
    return [newcontent,mode,simple]




##Sample Query translation
def create_sample_select(tokens,graph,mode,details):

    rr = random.randrange(int("".join(tokens.lb)),int("".join(tokens.sample))+1,1)
    wherestr = getwherestr(tokens.where)


    if mode =='node':
        star='id'

    else:
        star ='start, end'

    if details=='True':
        star='*'


    if len(wherestr)!=0:

        newcontent = ("select {ast} from {graphname} where "+wherestr+ 'ORDER BY Random() LIMIT {ns}').format(graphname=graph+"_"+mode,ns=rr,ast=star)

    else:

        newcontent = ("select {ast} from {graphname} ORDER BY Random() LIMIT {ns}").format(graphname=graph+"_"+mode,ns=rr,ast=star)


    return newcontent


##Compact Query translation
def create_compact_select(tokens,graph,mode,details):

    w = tokens.compactwhere
    newcontent =""

    star = 'id'
    if mode =='node':
        star='id'

    else:
        star ='start, end'

    if details=='True':
        star='*'

    for x in w :

        newcontent=newcontent +("select {ast} from {graphname} where "+" ".join(x)).format(graphname=graph+"_"+mode,ast=star)+";"

    newcontent = newcontent.rstrip(";")

    return newcontent

##Simple Query translation
def create_simple_select(tokens,graph,mode,details):

    wherestr = getwherestr(tokens.where)

    star = 'id'
    if mode =='node':
        star='id'

    else:
        star ='start, end'

    if details=='True':
        star='*'


    if len(wherestr)!=0:
        newcontent = ("select {ast} from {graphname} where "+wherestr).format(graphname=graph+"_"+mode,ast=star)

    else:

        newcontent = ("select {ast} from {graphname}").format(graphname=graph+"_"+mode,ast=star)

    return newcontent


##Mixed Query translation
def create_mixed_select(tokens,graph,list,details):


    wherestr = getwherestr(tokens.where)

    for idx, item in enumerate(list):
        if not ('v.' in item or 'u.' in item):
                 wherestr = wherestr.replace(item,'a.'+item)

    if details=='True':
        newcontent = ('select a.* from {graphname1} a,{graphname2} u, {graphname2} v  where a.start = u.id and a."end" = v.id  and '+wherestr).format(graphname1=graph+"_edge",graphname2=graph+"_node")
    else:
        newcontent = ('select a.start,a.end from {graphname1} a,{graphname2} u, {graphname2} v  where a.start = u.id and a."end" = v.id  and '+wherestr).format(graphname1=graph+"_edge",graphname2=graph+"_node")

    return newcontent

def create_mixed_select2(tokens,graph,list,details):


    wherestr = getwherestr(tokens.where)

    for idx, item in enumerate(list):
        if not ('e.' in item):
                 wherestr = wherestr.replace(item,'u.'+item)
    if details=='True':
        newcontent = ('select distinct u.* from {graphname1} e,{graphname2} u where u.id =e.start or u.id = e."end"   and '+wherestr).format(graphname1=graph+"_edge",graphname2=graph+"_node")
    else:
        newcontent = ('select distinct u.id from {graphname1} e,{graphname2} u where u.id =e.start or u.id = e."end"   and '+wherestr).format(graphname1=graph+"_edge",graphname2=graph+"_node")

    return newcontent

##Subgraph Query translation
def create_subgraph_select(tokens,graph,details):
    list = find_subgraph_select(tokens,graph)

    if details=='True':

      newcontent = ('select * from {graphname} where id in ({l})').format(graphname=graph+"_node",l=list)

    else:
      newcontent = ('select id from {graphname} where id in ({l})').format(graphname=graph+"_node",l=list)

    return newcontent


def find_subgraph_select(tokens,graph):
    d=tokens.depth
    s= tokens.startnode
    ids = [s]
    temp = []
    if int(d)==1:
     [ids.append(e) for e in get_neigbours(graph,s)]
    for i in range(1,int(d)):
     for id in ids:
          x= get_neigbours(graph,id)
          for j in x:
             if j not in ids:
                 ids.append(str(j))

    return ', '.join(ids)


##Set operation Query translation
def create_setop_select(tokens,graph,session,details):
    list = prepare_set_operation(tokens.setp,tokens.setname,session)

    if details=='True':

        newcontent = ('select * from {graphname} where id in ({l})').format(graphname=graph+"_node",l=list)

    else:

        newcontent = ('select id from {graphname} where id in ({l})').format(graphname=graph+"_node",l=list)

    return newcontent



##########


def get_neigbours(graph,s):
    db = sqlite3.connect(database_path)
    c = db.cursor()
    sqlt = ('select end from {graphname}_edge where start in (select id from {graphname}_node where id = {st}) union select start from {graphname}_edge where "end" in (select id from {graphname}_node where id = {st})  ').format(graphname=graph,st=s)
    c.execute(sqlt)
    data = c.fetchall()
    c.close()
    ids = []
    for e in data:
      ids.append(str(e[0]))
    return ids


def find_subgraph_select2(d,s,graph):
  ids = []


  if int(d)==1:
     [ids.append(e) for e in get_neigbours(graph,s)]
  elif int(d)>1:


    [ids.append(e) for e in get_neigbours(graph,s)]


    for i in range(1,int(d)):

       temp=[s]


       for id in ids:

          x= get_neigbours(graph,id)



          for j in x:

             if j not in temp :
                 temp.append(str(j))

       temp.pop(0)

       ids.extend(temp)



  return ', '.join(ids)


def satisfy_condition(list,graph,condition):
    db = sqlite3.connect('Edison.db')
    c = db.cursor()
    if condition:
     temp = 'and '+condition
    else:
     temp =''
    sqlt = ('select count(*) from {graphname}_node where id in ({l}) {cond} ').format(graphname=graph,l=list,cond=temp)

    c.execute(sqlt)
    data = c.fetchone()
    c.close()

    if data[0] > 0:
        return True

    else:
        return False


def get_nodes_nhbrs(gnodes,depth,graph,cond):
  temp=[]
  for i in gnodes:
    list= find_subgraph_select2(depth,i,graph)
    if satisfy_condition(list,graph,cond):
        temp.append(int(i))


  return temp

def nhbr_query(d,graph,cond):
    db = sqlite3.connect('Edison.db')
    c = db.cursor()
    sqlt = ('select id from {graphname}_node').format(graphname=graph)
    c.execute(sqlt)
    data = c.fetchall()
    c.close()
    ids = []
    for e in data:
      ids.append(str(e[0]))

    t =get_nodes_nhbrs(ids,d,graph,cond)

    return t


def find_subgraph_select3(graph,condition,depth,mode):

  if mode == 1:
      return set(nhbr_query(depth,graph,condition))

  else:
      return set(nhbr_query(depth,graph,condition)).difference(set(nhbr_query(depth-1,graph,condition)))
##########

def get_neigbours(graph,s):
    db = sqlite3.connect(database_path)
    c = db.cursor()
    sqlt = ('select end from {graphname}_edge where start in (select id from {graphname}_node where id = {st}) union select start from {graphname}_edge where "end" in (select id from {graphname}_node where id = {st})  ').format(graphname=graph,st=s)
    c.execute(sqlt)
    data = c.fetchall()
    c.close()
    ids = []
    for e in data:
      ids.append(str(e[0]))
    return ids


def create_set_select(tokens,graph,mode,details):

    star = 'id'
    if mode =='node':
        star='id'

    else:
        star ='start, end'

    if details=='True':
        star='*'


    i =0
    newcontent=''
    for m in tokens.setop:

        newcontent = newcontent+"select {ast} from {graphname} where "+getwherestr(tokens.where[i])+m+' '
        i=i+1

        newcontent = newcontent + "select {ast} from {graphname} where "+getwherestr(tokens.where[i])
        newcontent = newcontent.format(graphname=graph+"_"+mode,ast=star)


    return newcontent


##Execute the final SQL after translation
def execute_sql(sql_final,mode,graph):
    try:
     db = sqlite3.connect(database_path)
     c = db.cursor()

     c.execute(sql_final)

     data = c.fetchall()
     c.close()
     name,cols=getcolnames(mode,graph)

     cd = cols.split(",")
     mylist = []

     for e in data:
        d={}
        c={}
        for i in range(0,len(e)):
           d[cd[i].strip()]=e[i]
        mylist.append(d)


     num = getnodes_edges(graph)

     if mode=='node':
         coverarge = float(len(mylist))/num[0]
     else:
         coverarge = float(len(mylist))/num[1]

     coverarge = round(coverarge*100,1)

     result = {name:mylist,'coverage':coverarge}




     return result


    except sqlite3.Error as e:
        print "An error occurred:", e.args[0]




##Calculate the edit distance between two strings (attribute names)
def calcmind(c,att):

    cd = c.split(",")


    m =[]
    for i in range(0,len(cd)):
        m.append(lev(cd[i].lstrip(' ').rstrip(' '),att))

    if min(m) <0.5 and min(m)>0:
        x=cd[m.index(min(m))]

        return -1,x
    elif min(m)==0:

        x=cd[m.index(min(m))]

        return 1,x

    else:
        if checkifmeasureexist(att):
            return 2,"x"
        else:
            x=cd[m.index(min(m))]
            return 0,x


def checkifmeasureexist(att):
     db = sqlite3.connect(database_path)
     c1 = db.cursor()
     str1="select count(*) from measure where name = '{c}'".format(c=att)
     #print str1
     c1.execute(str1)
     data = c1.fetchone()
     c1.close()
     if int(data[0]) > 0:
         return True
     else:
         return False


def lev(s1, s2):
    if len(s1) < len(s2):
        return lev(s2, s1)

    # len(s1) >= len(s2)
    if len(s2) == 0:
        return len(s1)

    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1 # j+1 instead of j since previous_row and current_row are one character longer
            deletions = current_row[j] + 1       # than s2
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return (float)(previous_row[-1]/(float(len(s1))+float(len(s2))))

##Memoization Service
##Create Query signuate
def create_query_signature(q):
    q= q.replace(" ","")
    s= q.translate(None, whitespace)
    f=s.lower()
    return f

##Check if query already exist in the database
def check_query_exist(q,details,mode):

    q= q.replace(" ","")
    s= q.translate(None, whitespace)
    f=s.lower()
    db = sqlite3.connect(database_path)
    try:
      if mode==1:
        c1 = db.cursor()
        str1="select count(*) from query_repository where signature = '{c}' and details ='{d}'".format(c=f,d=details)

        c1.execute(str1)
        data = c1.fetchone()
        c1.close()
        if int(data[0]) > 0:
            return True
        else:
            return False
      else:
        c1 = db.cursor()
        str1="select query_id from query_repository where signature = '{c}' and details ='{d}'".format(c=f,d=details)

        c1.execute(str1)
        data = c1.fetchone()
        c1.close()
        return data[0]

    except MyError as e:
        return "Error"



##Get graph attributes names -- Edge or node
def getcolnames  (mode,graph):
    db = sqlite3.connect(database_path)
    c1 = db.cursor()
    if mode == 'node':
     str="SELECT node_attributes FROM network WHERE name='{gname}'".format(gname=graph)
     c1.execute(str)
     data = c1.fetchone()
     db.close()
     temp = data[0].replace("integer","")
     temp = temp.replace("text","")
     temp = temp.replace("real","")
     temp = temp.replace(";",",")
     if graph!='nrv':
       # if graph !='wikivote' and graph !='epinions' and graph !='slashdot':
            colnames = temp + ",closeness_centrality, load_centrality, node_clique_number, betweenness_centrality, clustering"

        #else:
         #   colnames = temp + ", closeness_centrality, load_centrality, node_clique_number, betweenness_centrality, clustering"

     else:
      #colnames = temp + ",degree, clustering"
       colnames = temp + ",closeness_centrality, load_centrality, node_clique_number, betweenness_centrality, clustering"


     return 'nodes',colnames

    else:
     str="SELECT edge_attributes FROM network WHERE name='{gname}'".format(gname=graph)
     c1.execute(str)
     data = c1.fetchone()
     db.close()
     if data[0]!="no":
        temp = data[0].replace("integer","")
        temp = temp.replace("text","")
        temp = temp.replace("real","")
        temp = temp.replace(";",",")
        if graph!='nrv':
            colnames = "start, end,"+temp+", betweenness_centrality"

        else:
            colnames = "start, end, "+temp


     else:
        colnames = "start, end, betweenness_centrality"


     return 'edges',colnames



def getwherestr(twhere):
    temp=''
    for x in twhere:
        if not isinstance(x, basestring):

         for y in x:

            if not isinstance(y, basestring):
                for h in y:

                 temp =temp+h+' '
            else:
                temp = temp + y+' '

        else:
            temp = temp + x+' '
    return temp


def checkEqual(iterator):
       return len(set(iterator)) <= 1


def getnodes_edges(graph):
    db = sqlite3.connect(database_path)
    c1 = db.cursor()
    str="SELECT nodes,edges FROM network WHERE name='{gname}'".format(gname=graph)
    c1.execute(str)
    data = c1.fetchone()
    db.close()

    return data

##Memoization Service
##Retrieve pre-stored query results
def get_query_result(content,v):
    db = sqlite3.connect(database_path)
    c1 = db.cursor()
    sig=create_query_signature(content)
    str="select result,view,count(*),query_id from query_repository where signature = '{c}'".format(c=sig)

    c1.execute(str)
    data = c1.fetchone()
    if int(data[2])>0:

     if v==data[1]:
      return data[0],data[3]
     else:
        return 'err','null'

    db.close()

##Store query results for future use
def store_query(content,graph,result,attr,simple,details):
  try:
   content =' '.join(content.split())
   if not check_query_exist(content,details,1):
    db = sqlite3.connect(database_path)
    c1 = db.cursor()
    if simple=='yes':
        view = 'property'
    else:
        view ='seed'

    sig = create_query_signature(content)

    str1="insert into query_repository (Content,Network_Name,Result,Target,View,Signature, Details) values ('{c}','{g}','{r}','{a}','{v}','{s}','{d}')".format(c=content,g=graph,r=result,a=attr,v=view,s=sig,d=details)

    c1.execute(str1)
    db.commit()
    lid = c1.lastrowid


    filename = query_path+graph+"_query"+str(lid)+".txt"
    # with io.open(filename, 'w', encoding='utf-8') as f:
    #     f.write(unicode(json.dumps(result, ensure_ascii=False)))
    db.close()
    #out_file = open(filename,"w")
    #json.dump(result,out_file)
    #out_file.close()
    index_query(lid,content,graph,filename,attr,view)
    return lid
   else:
       return 'exist'


  except MyError as e:
        return "Error"



##Query search service. Index Query using search engine
def index_query(id,content,network,results,target,view):


    try:

        if view =="property":

         ix = index.open_dir(index_path1)
         writer = ix.writer()
         writer.add_document(query_id=unicode(id),content=unicode(content),network_name=unicode(network),results=unicode(results),target=unicode(target))
         writer.commit()
        elif view =="seed":

         ix = index.open_dir(index_path2)
         writer = ix.writer()
         writer.add_document(query_id=unicode(id),content=unicode(content),network_name=unicode(network),results=unicode(results),target=unicode(target))
         writer.commit()

        return "ok"

    except MyError as e:
        return "Error : msg".format(msg=e.value)

def get_query_name():
    db = sqlite3.connect(database_path)
    c1 = db.cursor()
    str="select result from query_repository where"
    c1.execute(str)
    data = c1.fetchone()
    db.close()

    return data

#---------------------------------------------------

def prepare_set_operation(x,y,session):
    d=myfun(y,session)
    z=evalset(d,x)
    z2=repr(z).replace("set([","").replace("])","")
    return z2

def get_set(s,session):

    db = sqlite3.connect(database_path)
    c = db.cursor()
    sqlt = ("select query from result_set where setname='{c}' and session={d}").format(c=s,d=session)
    c.execute(sqlt)
    data = c.fetchone()
    sqlt = ("select result from query_repository where query_id='{c}'").format(c=data[0])
    c.execute(sqlt)
    data2 = c.fetchone()

    return data2[0]



def myfun(y,session):
 data=[]
 i = 0
 for j in y:
       data.append(parsej(json.loads(get_set(j,session))))

 return data


def parsej(j):
    a = set()
    for key in j.keys():
     for item in j[key]:

         for key2 in item.keys():
             if key2 == "nodes":
                for item2 in item[key2]:

                    a.add(item2["id"])


    return a


def evalset(d,x):
  m =0
  for e in x:
        temp = d[0]
        if e=='intersect':
              temp = temp & d[m+1]
        elif e=='union':
              temp = temp | d[m+1]

        elif e=='except':
              temp = temp - d[m+1]

        m = m+1

  return temp


