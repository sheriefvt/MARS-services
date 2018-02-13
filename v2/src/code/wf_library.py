
import ConfigParser,io

import  sqlite3

import numpy
import math

from mpipe import OrderedWorker
import requests, json
from bottle import error
from pythonds.basic.stack import Stack

# import matplotlib.pyplot as plt
# import matplotlib as mp

with open ('mars.config', "r") as myfile:
        data=myfile.read()
config = ConfigParser.RawConfigParser(allow_no_value=True)
config.readfp(io.BytesIO(data))
server = config.get("MARS_configuration", "server")
host = config.get("MARS_configuration", "host")
host2 = config.get("MARS_configuration", "host2")
port2 = config.get("MARS_configuration", "port2")
port4 = config.get("MARS_configuration", "port4")
database_path = config.get("MARS_configuration", "database")
index_path1 = config.get("MARS_configuration", "index1")
index_path2 = config.get("MARS_configuration", "index2")
workflow_path = config.get("MARS_configuration", "workflow")

var_storage=[]



##Class definition for network_data process. Selects network attribute data node or edge. Filter can be applied.
class network_data(OrderedWorker):
 def doTask(self,value):
    sid = value.pop()

    temp = value.pop()



    tmp = temp.split(",")

    #
    attribute =tmp[0]
    target=tmp[2]
    network =tmp[1]
    cond =tmp[3]

    #
    db = sqlite3.connect(database_path)
    c = db.cursor()

    if target=="node":

        if cond=="no condition":
            cstr  = ""
        else:
            cstr = "where "+cond

        str="select {name} from {n}_node {cd}".format(name=attribute,n=network,cd=cstr)

        c.execute(str)
        data=c.fetchall()
        value.push(data)
        value.push(sid)
        return value

    elif target=="edge":
         if cond=="no condition":
            cstr  = ""
         else:
            cstr = "where "+cond

         str="select {name} from {n}_edge {cd}".format(name=attribute,n=network,cd=cstr)

         c.execute(str)
         data=c.fetchall()

         value.push(data)
         value.push(sid)
         return value

    else:
        return 'error'


##Class definition for start process. Prepare the workflow input data and creates a session
class start(OrderedWorker):
    def doTask(self,value):

            sid = create_session()

            tmp = value.split("|")
            s = Stack()


            for i in range(len(tmp)-1,-1,-1):
              s.push(tmp[i])

            s.push(sid)
            return s

##Class definition for save_data process. Saves data into a variable (for a specific session) in the DB using the store_variable method.
class save_data(OrderedWorker):
    def doTask(self, value):

        sid = value.pop()
        tmp1 = value.pop()
        tmp2 = value.pop()
        store_variable(tmp2,tmp1,sid)
        #value.push(tmp1)
        value.push(sid)
        return value

##Class definition for load_data process. Loads data from the DB
class load_data(OrderedWorker):
    def doTask(self, value):

        sid = value.pop()
        tmp1 = value.pop()
        tmp2 = value.pop()
        d=load_variable(tmp2,sid)

        x=[]

        for i in d:
            x.append(eval(i[0]))


        value.push(x)
        value.push(sid)
        return value

##Class definition for execute_query process. Calls the query service using the rest api.
class execute_query(OrderedWorker):
    def doTask(self,value):
        try:

            sid = value.pop()

            tmp=value.pop()




            temp = tmp.split(",")
            content = temp[0]
            network = temp[1]
            target = temp[2]


            url ='http://'+host2+":"+port2+"/graphservice/query"
            data = {'content':content,'graph':network,'validate':'False','view':'property','parallel':'False','nruns':4,'memo':'False','details':True}

            r = requests.get(url,params=data)

            y = r.text

            z=json.loads(y).get('result')
            ar = z["{t}_sets".format(t=target)][0]["{t}s".format(t=target)]

            value.push(ar)
            value.push(sid)
            return value

        except error500 as e:
            return 'Error executing the query'





@error(500)
def error500(error):
    return 'Internal Error'






##Class definition for query_data process. Extract attribute values from query response (JSON)
class query_data(OrderedWorker):
    def doTask(self,value):
     sid = value.pop()

     temp1 = value.pop()
     temp2 = value.pop()

     #print 'Inside query data..'
     listx = []

     for i in temp1:
      listx.append(i[temp2])


     value.push(listx)
     value.push(sid)
     return value

##Class definition for end process. Terminates workflow session started by the start process and returns the final results
class end(OrderedWorker):
    def doTask(self,value):
     sid = value.pop()
     temp1 = value.pop()
     end_session(sid)
     return temp1

##Helper function used to convert data to numpy array.
def create_numpy_list(data):
    l=[]

    for d in data:

        l.append(d)
    return numpy.array(l)

##Class definitions for different data analyses using numpy package.
class sum_data(OrderedWorker):
    def doTask(self,value):
     sid = value.pop()
     s = value.pop()

     x = create_numpy_list(s)
     value.push(numpy.sum(x))
     value.push(sid)
     return value


class min_data(OrderedWorker):
    def doTask(self,value):
     sid = value.pop()
     s = value.pop()
     x = create_numpy_list(s)
     value.push(numpy.min(x))
     value.push(sid)
     return value

class max_data(OrderedWorker):
    def doTask(self,value):
     sid = value.pop()
     s = value.pop()
     x = create_numpy_list(s)
     value.push(numpy.max(x))
     value.push(sid)
     return value

class product_data(OrderedWorker):
    def doTask(self,value):
     sid = value.pop()
     s = value.pop()
     x = create_numpy_list(s)
     value.push(numpy.product(x))
     value.push(sid)
     return value

class percentile_data(OrderedWorker):
    def doTask(self,value):
     sid = value.pop()
     s = value.pop()
     s2  = value.pop()
     x = create_numpy_list(s)
     value.push(numpy.percentile(x,s2))
     value.push(sid)
     return value

class median_data(OrderedWorker):
    def doTask(self,value):
     sid = value.pop()
     s = value.pop()
     x = create_numpy_list(s)

     value.push(numpy.median(x))
     value.push(sid)
     return value


class average_data(OrderedWorker):
    def doTask(self,value):
     sid = value.pop()
     s = value.pop()
     x = create_numpy_list(s)

     value.push(numpy.average(x))
     value.push(sid)
     return value

class std_data(OrderedWorker):
    def doTask(self,value):
     sid = value.pop()
     s = value.pop()
     x = create_numpy_list(s)
     value.push(numpy.std(x))
     value.push(sid)
     return value

class count_data(OrderedWorker):
    def doTask(self,value):
     sid = value.pop()
     s = value.pop()
     x = create_numpy_list(s)
     value.push(x.size)
     value.push(sid)
     return value

class var_data(OrderedWorker):
    def doTask(self,value):
     sid = value.pop()
     s = value.pop()
     x = create_numpy_list(s)
     value.push(numpy.var(x))
     value.push(sid)
     return value

# class plot_data(OrderedWorker):
#     def doTask(self, value):
#
#      sid  = value.pop()
#
#      if value.size()==2:
#
#       temp1 = value.pop()
#
#
#       temp2 = value.pop()
#
#       inp = temp2.split(",")
#
#       if inp[0]=="1":
#           db = sqlite3.connect(database_path)
#           c = db.cursor()
#           str="insert into data_plot (parameters) values ('{v}')".format(v = inp[1]+"|"+inp[2]+"|"+inp[3]+"|"+inp[4]+"|"+inp[5]+"|"+inp[6]+"|"+inp[7]+"|"+inp[9]+"|"+inp[10]+"|"+inp[8])
#
#           c.execute(str)
#           db.commit()
#           lr = c.lastrowid
#           c.close()
#           plot_degree_distribution(temp1,lr,inp[1],inp[2],inp[3],inp[4],inp[5],inp[6],inp[7],inp[9],inp[10],inp[8])
#
#       elif inp[0]=="2":
#
#           db = sqlite3.connect(database_path)
#           c = db.cursor()
#           str="insert into data_plot (parameters) values ('{v}')".format(v = inp[1]+"|"+inp[2]+"|"+inp[3]+"|"+inp[4]+"|"+inp[5]+"|"+inp[6]+"|"+inp[7]+"|"+inp[8]+"|"+inp[10]+"|"+inp[11]+"|"+inp[9])
#
#           c.execute(str)
#           db.commit()
#           lr = c.lastrowid
#           c.close()
#           plot_clustering_distribution(temp1,lr,inp[9],inp[1],inp[2],inp[3],inp[4],inp[5],inp[6],inp[7],inp[10],inp[11],inp[8])
#
#       elif inp[0]=="4":
#
#           db = sqlite3.connect(database_path)
#           c = db.cursor()
#           str="insert into data_plot (parameters) values ('{v}')".format(v = inp[1]+"|"+inp[2]+"|"+inp[3]+"|"+inp[4]+"|"+inp[5])
#
#           c.execute(str)
#           db.commit()
#           lr = c.lastrowid
#           c.close()
#           plot_category_distribution(temp1,inp[1],inp[2],inp[3],lr,inp[4],inp[5])
#
#      else:
#
#
#
#       temp2 = value.pop()
#
#       inp = temp2.split(",")
#       if inp[0]=="3":
#
#           db = sqlite3.connect(database_path)
#           c = db.cursor()
#           str="insert into data_plot (parameters) values ('{v}')".format(v = inp[1]+"|"+inp[2]+"|"+inp[3]+"|"+inp[4]+"|"+inp[5]+"|"+inp[6]+"|"+inp[7])
#
#           c.execute(str)
#           db.commit()
#           lr = c.lastrowid
#           y = load_variable(inp[1],sid)
#           c.close()
#           plot_variable(inp[1].split(":"),y,inp[2].split(":"),lr,inp[3],inp[4],inp[5],inp[6],inp[7])
#
#      value.push(lr)
#      value.push(sid)
#      return value
#
# def plot_clustering_distribution (gnodes,figid,nb,yl,xl,xfontsize,tfontsize,mc,mt,ms,ls,lw,title):
#     gnodes2 = [i[0] for i in gnodes]
#     new_items = [x if x !=1 else 0.9999999999 for x in gnodes2]
#
#     xbins = numpy.linspace(0, 1,nb)
#
#     x = create_numpy_list(new_items)
#     digitized = numpy.digitize(x, xbins)
#     bin_size = [len(x[digitized == i]) for i in range(1, len(xbins))]
#
#     plt.xlabel(xl, fontsize=int(xfontsize))
#     plt.ylabel(yl, fontsize=int(xfontsize))
#     plt.yscale("log")
#     plt.ticklabel_format(style='sci',axis='x',scilimits=(0,0))
#     plt.yticks(size= int(tfontsize))
#     plt.xticks(size = int(tfontsize))
#     plt.xlim(0,0.05,1)
#
#
#     new_bins = []
#     for i in range(0,len(xbins)-1):
#         new_bins.append((xbins[i]+xbins[i+1])/2)
#     lines = ls
#
#
#
#     plt.plot(new_bins,bin_size,lines,c=mc,marker=mt,markersize=int(ms),linewidth=int(lw))
#     with open(workflow_path+"{fi}.dat".format(fi=figid), 'w') as f:
#      f.write('Clustering Coefficient,Frequency\n')
#      output = numpy.column_stack((numpy.array(new_bins).flatten(),numpy.array(bin_size).flatten()))
#      numpy.savetxt(f,output,delimiter=',',fmt='%s')
#     plt.ylim(ymin=0)
#     plt.xlim(xmin = 0,xmax=1)
#     plt.title(title,fontsize=int(xfontsize))
#     plt.savefig(workflow_path+"{fi}.png".format(fi=figid),bbox_inches='tight')
#     plt.savefig(workflow_path+"{fi}.pdf".format(fi=figid),bbox_inches='tight')
#     plt.close()
#
#
#
# def plot_degree_distribution (gnodes,figid,yl,xl,xfontsize,tfontsize,mc,mt,ms,ls,lw,title) :
#
#     gnodes2 = [i[0] for i in gnodes]
#
#     degs = {}
#     for n in gnodes2:
#
#         if n not in degs:
#             degs[n] = 0
#         degs[n] += 1
#
#     items = sorted(degs.items())
#
#     plt.xticks( size= int(tfontsize),rotation=40)
#     plt.yticks(size= int(tfontsize))
#     plt.yscale("log")
#     plt.xscale("log")
#     mflag = False
#     kflag = False
#     lines = ls
#     x= [k for (k , v) in items]
#     y = [v for (k,v) in items]
#     for i in y:
#         if len(str(math.modf(float(i))[1]))-2 >6:
#             mflag = True
#
#         elif  len(str(math.modf(float(i))[1]))-2 >3:
#             kflag = True
#
#     #print [float(x) / 1000000 for x in vavalues]
#     if mflag:
#         yl = yl + " (in millions)"
#         newList = [float(h) / 1000000 for h in y]
#     elif kflag:
#         yl = yl + " (in thousands)"
#         newList = [float(h) / 1000 for h in y]
#     else:
#         newList = [float(h) for h in y]
#
#     plt.plot(x,newList,lines,c=mc,marker=mt,markersize=int(ms),linewidth=int(lw))
#     plt.xlabel(xl, fontsize=int(xfontsize))
#     plt.ylabel(yl, fontsize=int(xfontsize))
#     with open(workflow_path+"{fi}.dat".format(fi=figid), 'w') as f:
#      f.write('Degree,Frequency\n')
#      output = numpy.column_stack((numpy.array([ k  for (k , v) in items]).flatten(),numpy.array([v for (k,v) in items]).flatten()))
#      numpy.savetxt(f,output,delimiter=',',fmt='%s')
#     plt.ylim(0,80,20)
#     plt.xlim(0,40,5)
#     plt.title (title,fontsize = xfontsize)
#     plt.savefig (workflow_path+"{fi}.png".format(fi=figid),bbox_inches='tight')
#     plt.savefig (workflow_path+"{fi}.pdf".format(fi=figid),bbox_inches='tight')
#     plt.close()
#
#
# def plot_category_distribution (gnodes,category,labels,colors,figid,xfontsize,title) :
#
#     mp.rcParams['font.size'] = 25
#     x = [i[0] for i in gnodes]
#
#     g = category.split(":")
#     nlabels = labels.split(":")
#     ncolors  = colors.split(":")
#     cs = []
#     for z in g:
#
#       cs.append(len([i for i in x if eval(str(i)+" "+z)]))
#
#     if sum(cs) < len(x):
#        cs.append(len(x) -sum(cs) )
#        nlabels.append("Others")
#        ncolors.append("silver")
#     plt.pie(cs, labels=nlabels, colors=ncolors,
#         autopct='%1.1f%%', shadow=False, startangle=90,pctdistance=0.8)
#
#
#
#     with open(workflow_path+"{fi}.dat".format(fi=figid), 'w') as f:
#      f.write('Category,Size\n')
#      output = numpy.column_stack((numpy.array(nlabels).flatten(),numpy.array(cs).flatten()))
#      numpy.savetxt(f,output,delimiter=',',fmt='%s')
#
#     plt.title (title,fontsize = xfontsize)
#     plt.savefig (workflow_path+"{fi}.png".format(fi=figid),bbox_inches='tight')
#     plt.savefig (workflow_path+"{fi}.pdf".format(fi=figid),bbox_inches='tight')
#     plt.close()
#
#
#
#
# def plot_variable (varnames,vavalues,colors,figid,yl,xl,xfontsize,tfontsize,title) :
#
#     mflag = False
#     kflag = False
#     for i in vavalues:
#         if len(str(math.modf(float(i))[1]))-2 >6:
#             mflag = True
#
#         elif  len(str(math.modf(float(i))[1]))-2 >3:
#             kflag = True
#
#
#     if mflag:
#         yl = yl + " (in millions)"
#         newList = [float(x) / 1000000 for x in vavalues]
#     elif kflag:
#         yl = yl + " (in thousands)"
#         newList = [float(x) / 1000 for x in vavalues]
#     else:
#         newList = [float(x) for x in vavalues]
#
#     y_pos = numpy.arange(len(varnames))
#
#
#     plt.bar(y_pos, newList, align='center', color =colors)
#     with open(workflow_path+"{fi}.dat".format(fi=figid), 'w') as f:
#      f.write('Variable,Value\n')
#      output = numpy.column_stack((numpy.array(varnames).flatten(),numpy.array(vavalues).flatten()))
#      numpy.savetxt(f,output,delimiter=',',fmt='%s')
#     plt.xticks(y_pos, varnames)
#     plt.xticks(size= int(tfontsize),rotation=40)
#     plt.yticks(size= int(tfontsize))
#     plt.xlabel(xl, fontsize=int(xfontsize))
#
#
#     plt.ylabel(yl, fontsize=int(xfontsize))
#     plt.title (title,fontsize=int(xfontsize))
#
#
#     plt.savefig (workflow_path+"{fi}.png".format(fi=figid),bbox_inches='tight')
#     plt.savefig (workflow_path+"{fi}.pdf".format(fi=figid),bbox_inches='tight')
#     plt.close()
#

def create_session():

      db = sqlite3.connect(database_path)
      c = db.cursor()
      str="insert into session (status) values ('active')"
      #print str
      c.execute(str)
      db.commit()
      c.close()
      lr = c.lastrowid
      return lr

def end_session(id):

      db = sqlite3.connect(database_path)
      c = db.cursor()
      str="update session set status = 'inactive' where id = {d}".format(d=id)
      #print str
      c.execute(str)
      db.commit()
      c.close()

      db = sqlite3.connect(database_path)
      str="delete from variable_storage where session = {d}".format(d=id)
      #print str
      c = db.cursor()
      c.execute(str)
      db.commit()
      c.close()


def store_variable(n,v,s):

      db = sqlite3.connect(database_path)
      c = db.cursor()
      str="insert into variable_storage (variable,value,session) values ('{nn}','{vv}','{ss}')".format(nn=n,vv=v,ss=s)
      #print str
      c.execute(str)
      db.commit()
      c.close()

def load_variable(n,s):

      db = sqlite3.connect(database_path)
      c = db.cursor()
      f = n.split(":")
      d=""
      for i in f:
        d= d+ '"'+i+'"'+','
      d = d[:-1]

      str="select value,variable from variable_storage where variable in ({nn}) and session = {ss}".format(nn=d,ss=s)
      #print str
      c.execute(str)
      data = c.fetchall()
      #print data
      x=[]
      y=[]
      for i in data:
        x.append(eval(i[0]))


      return x








