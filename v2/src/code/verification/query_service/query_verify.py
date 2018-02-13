__author__ = 'sherief'

import requests

import ConfigParser,io,time

with open ('mars.config', "r") as myfile:
        data=myfile.read()
config = ConfigParser.RawConfigParser(allow_no_value=True)
config.readfp(io.BytesIO(data))
host2 = config.get("MARS_configuration", "host2")
port2 = config.get("MARS_configuration", "port2")

print 'Start verification testing for network query service'


urlprod1 ='http://'+host2+":"+port2+"/graphservice/query"

urlprod2 = 'http://'+host2+":"+port2+"/graphservice/search"

urlprod3  = 'http://'+host2+":"+port2+"/graphservice/repository"


urlprod4 = 'http://'+host2+":"+port2+"/graphservice/storage/graph/filter"



data1 = {'content': "select nodes from sample",'graph':'sample','validate':'False','view':'property','parallel':'False','nruns':5,'memo':'False','details':True,'session':1}
data2 = {'content': "select nodes from sample where degree >2",'graph':'sample','validate':'False','view':'property','parallel':'False','nruns':5,'memo':'False','details':False,'session':1}


data3 = {'keywords': "sample",'metadata':'content','view':'property'}


data4 = {'type':'node','view':'property'}


data5 = {'attribute':'nodes','operator':'<','rvalue':'50'}
print ''
print 'executing simple query with details'

r1 = requests.get(urlprod1,params=data1)
x='{"qid": 0, "result": {"node_sets": [{"nodes": [{"clustering": 0.0, "node_clique_number": 2, "degree": 2, "closeness_centrality": 0.555555555556, "clustering_galib": 0.0, "load_centrality": 0.6, "id": 1, "betweeness_centrality": 0.6}, {"clustering": 0.0, "node_clique_number": 2, "degree": 2, "closeness_centrality": 0.454545454545, "clustering_galib": 0.0, "load_centrality": 0.4, "id": 2, "betweeness_centrality": 0.4}, {"clustering": 0.0, "node_clique_number": 2, "degree": 2, "closeness_centrality": 0.454545454545, "clustering_galib": 0.0, "load_centrality": 0.4, "id": 3, "betweeness_centrality": 0.4}, {"clustering": 0.0, "node_clique_number": 2, "degree": 2, "closeness_centrality": 0.555555555556, "clustering_galib": 0.0, "load_centrality": 0.6, "id": 4, "betweeness_centrality": 0.6}, {"clustering": 0.0, "node_clique_number": 2, "degree": 1, "closeness_centrality": 0.333333333333, "clustering_galib": 0.0, "load_centrality": 0.0, "id": 5, "betweeness_centrality": 0.0}, {"clustering": 0.0, "node_clique_number": 2, "degree": 1, "closeness_centrality": 0.333333333333, "clustering_galib": 0.0, "load_centrality": 0.0, "id": 6, "betweeness_centrality": 0.0}], "coverage": 100.0}]}}'

if r1.text==x:
    print "query results are valid"
else:
    print "query results are invalid"

print ''
print 'executing simple query without details'

r1 = requests.get(urlprod1,params=data2)
x='{"qid": 0, "result": {"node_sets": [{"nodes": [], "coverage": 0.0}]}}'
if r1.text==x:
    print "query results are valid"
else:
    print "query results are invalid"

print ''
print 'executing a network query search'
r2 = requests.get(urlprod2,params=data3)
x='[{"content": "select nodes from sample", "graph": "sample", "query_id": "2527", "results": "/home/sipcinet/edison/graphservices/query/sample_query2527.txt", "target": "node"}, {"content": "select nodes from sample where degree >2", "graph": "sample", "query_id": "2528", "results": "/home/sipcinet/edison/graphservices/query/sample_query2528.txt", "target": "node"}]'
if r2.text==x:
    print "query search results are valid"
else:
    print "query search results are invalid"

print ''
print 'list network query repository by node'
r3 = requests.get(urlprod3,params=data4)
x='{"node_queries": [{"content": "select nodes from sample where degree >2", "graph": "sample", "query_id": "2528", "results": "/home/sipcinet/edison/graphservices/query/sample_query2528.txt", "target": "node"}, {"content": "select nodes from sample", "graph": "sample", "query_id": "2527", "results": "/home/sipcinet/edison/graphservices/query/sample_query2527.txt", "target": "node"}]}'
if r3.text==x:
    print "Categorizing results are valid"
else:
    print "Categorizing results are invalid"

print ''
print 'network filtering'
r4 = requests.get(urlprod4,params=data5)
x='[{"directed": "false", "weighted": "false", "graph_id": 38, "name": "karate", "edge_attributes": {"degree_product": "integer", "betweenness_centrality": "real"}, "numberOfEdges": 78, "file_name": "karate", "original_format": "uel", "labeled": "true", "node_attributes": {"node_clique_number": "integer", "closeness_centrality": "real", "degree": "integer", "betweenness_centrality": "real", "load_centrality": "real", "id": "integer", "clustering": "real"}, "numberOfNodes": 34, "description": "Network of friendships between the 34 members of a karate club at a US university, as described by Wayne Zachary in 1977"}]'

if r4.text==x:
    print "filtering results are valid"
else:
    print "filtering results are invalid"

print ''


