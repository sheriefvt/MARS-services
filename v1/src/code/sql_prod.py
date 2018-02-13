from pyparsing import Literal, CaselessLiteral, Word, Upcase, delimitedList, Optional, \
    Combine, Group, alphas, nums, alphanums, ParseException, Forward, oneOf, quotedString, \
    ZeroOrMore, restOfLine, Keyword, Suppress

import sqlite3,sys,json,random,io
from whoosh import index
from string import whitespace
import ConfigParser,io

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
database_path = config.get("MARS_configuration", "database")
index_path1 = config.get("MARS_configuration", "index1")
index_path2 = config.get("MARS_configuration", "index2")
query_path = config.get("MARS_configuration", "query")

# define SQL-like language

def build_grammer():

    selectStmt = Forward()
    compoundselectStmt = Forward()
    subgraphselectStmt = Forward()
    sampleselectStmt = Forward()

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




    selectStmt      << ( selectToken +
                     targetToken.setResultsName( "type" ) +
                     fromToken +
                     (ident.setResultsName( "graph")))

    sampleselectStmt  << ( selectToken +samplestmt+
                     targetToken.setResultsName( "type") +
                     fromToken +
                     (ident.setResultsName( "graph"))+
                     Optional(whereToken + (whereExpression).setResultsName("where", listAllMatches=True) ))



    subgraphselectStmt  << ( selectToken +subgraphstmt +
                     fromToken +
                     (ident.setResultsName( "graph")))



    compoundselectStmt << selectStmt.setResultsName("select",listAllMatches=True) + ZeroOrMore(setop.setResultsName("setop",listAllMatches=True)  + selectStmt )



    SQL = sampleselectStmt|compoundselectStmt|subgraphselectStmt



    bSQL = SQL
    SqlComment = "--" + restOfLine
    bSQL.ignore( SqlComment )

    return bSQL


##Build grammer for SQL-like language.
bSQL = build_grammer()

##end point for the parsing service. Currently, we parse sql-like queries only. Can be extended in the future.
def parse_query( str,graph,runs,view):

    try:

        tokens = bSQL.parseString( str,parseAll=True )

        print "tokens = ",        tokens
        print "tokens.column=", tokens.column
        print "tokens.friends=", tokens.friends
        print "tokens.startnode=", tokens.startnode
        print "tokens.depth=", tokens.depth
        print "tokens.type =", tokens.type
        print "tokens.graph =",  tokens.graph
        print "tokens.where =", tokens.where
        print "tokens.setop =",tokens.setop
        print "tokens.defop =",tokens.defop
        print "tokens.batchop=",tokens.batchop
        print "tokens.select=",tokens.select
        print "tokens.compactwhere=",tokens.compactwhere
        print "tokens.sample=",tokens.sample
        print "tokens.runs=",tokens.runs
        print "tokens.lb=",tokens.lb


        if is_validgraph(tokens,graph)  and is_validview(tokens,runs,view):
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
        if int(tokens.lb) > int(tokens.sample) :
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


    return passed,tokens

##Query Translation to SQL
def transform(tokens,graph):

    mode = 'node'
    simple = 'yes'
    newcontent =''
    dd = tokens.type
    dd2 = tokens.setop
    dd3 = tokens.compactwhere
    dd4 = tokens.sample
    dd5 = tokens.runs
    dd6 = tokens.column

    if 'nodes' in dd or 'node' in dd :

        mode = 'node'

        if len(dd2)==0:

            if len(dd3)==0:
                if len(dd4)==0:
                    if any("e." in s for s in dd6):
                        newcontent = create_mixed_select2(tokens,graph,dd6)
                    else:
                        newcontent = create_simple_select(tokens,graph,'node')
                else:

                 if len(dd5)==0:
                  newcontent = create_sample_select(tokens,graph,'node')
                 else:
                  simple = 'run'
                  newcontent = create_sample_select(tokens,graph,'node')
            else:

                newcontent = create_compact_select(tokens,graph,'node')
                simple='no'
        else:


            newcontent =create_set_select(tokens,graph,'node')
    elif 'edges' in dd or 'edge' in dd :
        mode = 'edge'

        if len(dd2)==0:

            if len(dd3)==0:
                if len(dd4)==0:
                    if any("v." in s for s in dd6) or any("u." in s for s in dd6):


                        newcontent = create_mixed_select(tokens,graph,dd6)

                    else:

                        newcontent = create_simple_select(tokens,graph,'edge')


                else:
                 if len(dd5)==0:
                  newcontent = create_sample_select(tokens,graph,'edge')
                 else:
                  simple = 'run'
                  newcontent = create_sample_select(tokens,graph,'edge')
            else:

                newcontent = create_compact_select(tokens,graph,'edge')
                simple='no'
        else:


            newcontent =create_set_select(tokens,graph,'edge')

    elif 'subgraph' in dd :
        mode = 'node'
        newcontent =create_subgraph_select(tokens,graph)



    return [newcontent,mode,simple]




##Sample Query translation
def create_sample_select(tokens,graph,mode):


    nr =tokens.runs
    newcontent = ""
    for i in range(int(nr)):
        rr = random.randrange(int("".join(tokens.lb)),int("".join(tokens.sample))+1,1)
        wherestr = getwherestr(tokens.where)

        if len(wherestr)!=0:

           newcontent = newcontent+","+ ("select * from {graphname} where "+wherestr+ 'ORDER BY Random() LIMIT {ns}').format(graphname=graph+"_"+mode,ns=rr)

        else:

           newcontent = newcontent+","+ ("select * from {graphname} ORDER BY Random() LIMIT {ns}").format(graphname=graph+"_"+mode,ns=rr)

    
    return newcontent

##Compact Query translation
def create_compact_select(tokens,graph,mode):

    w = tokens.compactwhere
    newcontent =""
    for x in w :

        newcontent=newcontent +("select * from {graphname} where "+" ".join(x)).format(graphname=graph+"_"+mode)+";"

    newcontent = newcontent.rstrip(";")

    return newcontent

##Simple Query translation
def create_simple_select(tokens,graph,mode):

    wherestr = getwherestr(tokens.where)

    if len(wherestr)!=0:
        newcontent = ("select * from {graphname} where "+wherestr).format(graphname=graph+"_"+mode)

    else:

        newcontent = ("select * from {graphname}").format(graphname=graph+"_"+mode)

    return newcontent

##Mixed Query translation for edges
def create_mixed_select(tokens,graph,list):


    wherestr = getwherestr(tokens.where)

    for idx, item in enumerate(list):
        if not ('v.' in item or 'u.' in item):
                 wherestr = wherestr.replace(item,'a.'+item)
    newcontent = ('select a.* from {graphname1} a,{graphname2} u, {graphname2} v  where a.start = u.id and a."end" = v.id  and '+wherestr).format(graphname1=graph+"_edge",graphname2=graph+"_node")

    return newcontent

##Mixed Query translation for nodes
def create_mixed_select2(tokens,graph,list):


    wherestr = getwherestr(tokens.where)

    for idx, item in enumerate(list):
        if not ('e.' in item):
                 wherestr = wherestr.replace(item,'u.'+item)
    newcontent = ('select distinct u.* from {graphname1} e,{graphname2} u where u.id =e.start or u.id = e."end"   and '+wherestr).format(graphname1=graph+"_edge",graphname2=graph+"_node")

    return newcontent

##Subgraph Query translation
def create_subgraph_select(tokens,graph):
    list = find_subgraph_select(tokens,graph)
    newcontent = ('select * from {graphname} where id in ({l})').format(graphname=graph+"_node",l=list)
    return newcontent

##Subgraph Query
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

##########

##Subgraph Query
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

##Set Query Translator
def create_set_select(tokens,graph,mode):
    i =0
    newcontent=''
    for m in tokens.setop:

        newcontent = newcontent+"select * from {graphname} where "+getwherestr(tokens.where[i])+m+' '
        i=i+1

        newcontent = newcontent + "select * from {graphname} where "+getwherestr(tokens.where[i])
        newcontent = newcontent.format(graphname=graph+"_"+mode)


    return newcontent

##Execute the final SQL after translation
def execute_sql(sql_final,mode,graph):
    try:
     db = sqlite3.connect(database_path)
     c = db.cursor()
     sql_final = check_details(graph,sql_final,mode)

     c.execute(sql_final)

     data = c.fetchall()
     c.close()
     name,cols=getcolnames(mode,graph)

     cd = cols.split(",")
     mylist = []
     isSimple  = check_simple(graph)

     for e in data:
        d={}
        si =""
        if not isSimple:

            for i in range(0,len(e)):
               d[cd[i].strip()]=e[i]
            mylist.append(d)
        else:

            for i in range(0,len(e)):
               si = si+ str(e[i])+" "
            mylist.append(si.strip())


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


##Check if graph enable showing query details
def check_details(graph,stmt,mode):
     db = sqlite3.connect(database_path)
     c1 = db.cursor()
     str1="select details from network where name = '{c}'".format(c=graph)
     c1.execute(str1)
     data = c1.fetchone()
     if data[0]=="false":
         if mode =="node":
            stmt = stmt.replace("*","id")
         else:
             stmt = stmt.replace("*","start, end")
     c1.close()
     return stmt

##Check if graph enable showing simple query format
def check_simple(graph):
     db = sqlite3.connect(database_path)
     c1 = db.cursor()
     str1="select simple from network where name = '{c}'".format(c=graph)
     c1.execute(str1)
     data = c1.fetchone()
     c1.close()
     if data[0]=="false":
         return False
     else:
         return True

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
        x=cd[m.index(min(m))]
        return 0,x



##Calculate the levenshtein distance
def lev(s1, s2):
    if len(s1) < len(s2):
        return lev(s2, s1)


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
def check_query_exist(q):

    q= q.replace(" ","")
    s= q.translate(None, whitespace)
    f=s.lower()
    try:
     db = sqlite3.connect(database_path)
     c1 = db.cursor()
     str1="select count(*) from query_repository where signature = '{c}'".format(c=f)
     c1.execute(str1)
     data = c1.fetchone()
     c1.close()
     if int(data[0]) > 0:
         return True
     else:
         return False

    except MyError as e:
        return "Error"


def check_details(graph,stmt,mode):
     db = sqlite3.connect(database_path)
     c1 = db.cursor()
     str1="select details from network where name = '{c}'".format(c=graph)
     c1.execute(str1)
     data = c1.fetchone()
     if data[0]=="false":
         if mode =="node":
            stmt = stmt.replace("*","id")
         else:
             stmt = stmt.replace("*","start, end")
     c1.close()
     return stmt

##Get graph attributes names -- Edge or node
def getcolnames(mode,graph):
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

            colnames = temp + ",closeness_centrality, load_centrality, node_clique_number, betweenness_centrality, clustering"


     else:
      colnames = temp + ",degree, clustering"


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
    str="select result,view from query_repository where signature = '{c}'".format(c=sig)

    c1.execute(str)
    data = c1.fetchone()

    if v==data[1]:
     return data[0]
    else:
        return 'err'

    db.close()

##Store query results for future use
def store_query(content,graph,result,attr,simple):
  try:
   content =' '.join(content.split())
   if not check_query_exist(content):
    db = sqlite3.connect(database_path)
    c1 = db.cursor()
    if simple=='yes':
        view = 'property'
    else:
        view ='seed'

    sig = create_query_signature(content)
    str1="insert into query_repository (Content,Network_Name,Result,Target,View,Signature) values ('{c}','{g}','{r}','{a}','{v}','{s}')".format(c=content,g=graph,r=result,a=attr,v=view,s=sig)

    c1.execute(str1)
    db.commit()
    lid = c1.lastrowid


    filename = query_path+graph+"_query"+str(lid)+".txt"

    db.close()

    index_query(lid,content,graph,filename,attr,view)



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






#print parse_query("select sample(number = 3,size =[1,8])nodes from karate where degree > 5",'karate','3','seed')
# parse_query("select nodes from karate where degree > 5",'karate','20','seed')

#parse_query("karate.nodes={degree = 4, clustering  = 0.5}",'karate')
#print parse_query("select nodes from netscience where closeness_centrality > 1 and degr =4",'netscience' )

#print check_transform(t,"select nodes from karate where degree > 5 and node_clique_number > 2 intersect select nodes from karate where id < 3 union select nodes from karate where id < 3",'karate')

#parse_query( " :x=(age,height) :y=(5,10) | SELECT nodes from karate where :x  > :y ",'karate')
#parse_string("select nodes from graph where age > 4 || select nodes from graph where age =3")
#test( "select nodes from gdscalc_study_main sample(4) where x = 4" )
#parse_query("select nodes from gdscalc_study_main sample(1.5) where x = 4",'karate')

#tokens = test( "get nodes from SYSXYZZY where z=2 and y=3 intersection select nodes from SYSXYZZY where z=2 and y=3 intersection select nodes from sss" )
