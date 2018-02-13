__author__ = 'Sherif'
import requests, json
import time

url = "http://localhost:8082/graphservice/query"
urlnew = "http://edisondev.vbi.vt.edu:9000/graphservice/query"
urlprod = "http://edison.vbi.vt.edu:9000/graphservice/query"
urlprod2 = "http://sfx1.vbi.vt.edu:38080/graphservice/query"

url2 = "http://localhost:8081/graphservice/search"
url2new = "http://edisondev.vbi.vt.edu:9000/graphservice/search"
url24new = "http://localhost:8080/graphservice/search/createindex_backup"
url24new2 = "http://edison.vbi.vt.edu:9000/graphservice/search/createindex"
urlin = "http://edisondev.vbi.vt.edu:9000/graphservice/search/createindex"
url25new = "http://edisondev.vbi.vt.edu:9000/graphservice/repository"
url26new = "http://edisondev.vbi.vt.edu:9000/graphservice/directory"
url3 = "http://localhost:8081/graphservice/repository"
urlt = "http://localhost:8081/graphservice/storage/graph/getattribute?target=node&network=Karate"
url4 = "http://localhost:8081/graphservice/storage/graph"
url4new = "http://edisondev.vbi.vt.edu:9000/graphservice/storage/graph/filter"
url5 = "http://localhost:8082/graphservice/storage/graph/search"
url6 = "http://edisondev.vbi.vt.edu:9000/graphservice/storage/graph"
url7 = "http://localhost:8080/graphservice/storage/upload"
url8 = "http://localhost:8081/graphservice/model"
url9 = "http://localhost:8080/graphservice/plot/graph"
url10 = "http://localhost:8080/graphservice/plot/query"
url11 = "http://localhost:8081/graphservice/measure/compute"
url11sfx1 = "http://sfx1.vbi.vt.edu:38080/graphservice/measure/compute"

url12 = "http://sfx2.vbi.vt.edu:38080/graphservice/measure/compute"
url20 = "http://sfx2.vbi.vt.edu:38080/graphservice/storage/upload"
url14 = "http://edison.vbi.vt.edu:9000/graphservice/search/deleteindex"
#url14 = "http://localhost:8080/graphservice/search/createindex"
url15 = "http://localhost:8080/graphservice/search"
url22 = "http://localhost:8081/graphservice/repository"
url21 = "http://sfx2.vbi.vt.edu:38080/graphservice/storage/largeupload"
url23 = "http://localhost:8081/graphservice/storage/graph"

urlworkflow = "http://localhost:8081/workflowservice/execute"
urlworkflow2 = "http://sfx3.vbi.vt.edu:38080/workflowservice/execute"
datawf = {'wf':"select_attribute,max_data",'input':"degree,dolphins,node"}
datawf2 = {'wf':"start,execute_query",'input':"1;2"}

urladdnetwork = "http://sfx2.vbi.vt.edu:38080/graphservice/storage/addnetwork"

url27new ="http://sfx2.vbi.vt.edu:38080/graphservice/measure/addfunction"

urlh = "http://localhost:8081/heartbeat"
urlh2 = "http://edisondev.vbi.vt.edu:9001/heartbeat"
urlrng = "http://localhost:8080/graphservice/rng/triangular"
urltest = "http://localhost:8081/graphservice/visualization"
urld = "http://localhost:8080/download"

urlsave = "http://localhost:8081/graphservice/setoperation/save"
datasave = {'set':'setc','qid':'2069','sid':'1'}

urldelete = "http://localhost:8081/graphservice/setoperation/delete"
datadelete = {'setid':'1'}
urlsession = "http://localhost:8081/graphservice/session/create"
datasession = {'appid':'1'}
urlsession2 = "http://localhost:8081/graphservice/session/end"
datasession2 = {'aid':'1','sid':'1'}

datarng = {'left':1.3,'mode':3,'right':4.2,'size':100}


data13 = {'content': "select nodes from karate where degree <11",'graph':'karate','validate':'False','view':'property','parallel':'False','nruns':80,'memo':'False','details':True,'session':1}
#data13 = {'content': "select sample(number=12, size=[3,8])nodes from karate where degree > 5",'graph':'karate','validate':'False','view':'seed','parallel':'False','nruns':'12','memo':'True','details':False,'session':1}

dataset = {'content': "select edges from wikivote",'graph':'wikivote','validate':'False','view':'property','parallel':'False','nruns':'3','csvf':'False','memo':'False','details':False}


data33 = {'content': "select nodes from er7",'graph':'er7','validate':'False','view':'property','parallel':'False','details':'False'}
data14 = {'content': "select sample(5,5,random)      edges from karate",'graph':'karate','validate':'False','view':'seed'}
data16 = {'content': "select edges from netscience degree_product > 5",'graph':'netscience','validate':'False','view':'property','parallel':'False','session':1,'details':'False'}
data = {'content': "karate.nodes={degree = 4, clustering  = 0.5}",'graph':'karate','validate':'False'}
data12 = {'content': "karate.nodes={degree = 4, clustering  = 0.5}",'graph':'karate','validate':'False','view':'property','parallel':'False','session':1,'nruns':0}
data2 = {'keywords': "netscience",'metadata':'content','view':'property'}
data5 = {'name':"nrv"}
data6 = {'attribute':'nodes','operator':'>','rvalue':'400'}
dataaddnetwork = {'name':'sample'}
data9 = {'name':'nrv','format':'png'}
data10 = {'content':'select nodes where degree >3','format':'png'}
data11={'graph':'karate','measure':'9'}
datah={'action':'start'}
data15={'keywords':'select sample(4,2,fixed)nodes from karate where degree <2','metadata':'content','view':'seed'}
data25new={'type':'all','view':'property'}
datain={'view':'seed'}
datadin={'id':'2403','view':'property'}
data22={'view':'property'}
data27={'measure':'degree_product','file':'testm','num':2,'type':'integer','graph':'nrv','attribute':'start,end','target':'edge','function':'degree_prod'}
datatest={'mode':'ttype'}
start = time.time()
datass = {'user':'no'}
datat = {'network':'karate','target':'node'}

r = requests.get(url,params=data13)



e = time.time()-start
import zlib
print r.text
print e
#print r.json