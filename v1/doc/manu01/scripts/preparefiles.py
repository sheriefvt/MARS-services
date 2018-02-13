__author__ = 'Sherif'



import networkx as nx

import sys

name = sys.argv[1]

f1 = open('/home/sipcinet/edison/graphservices/graphs/new_graphs2/{n}.uel'.format(n=name), "r")
f2 = open('/home/sipcinet/edison/graphservices/graphs/new_graphs2/{n}.nodes'.format(n=name), "r")
f3 = open('/home/sipcinet/edison/graphservices/graphs/new_graphs2/{n}.info'.format(n=name), "w")
f4 = open('/home/sipcinet/edison/graphservices/graphs/new_graphs2/{n}.uel2'.format(n=name), "w")


lines1 = f1.readlines()
lines2  = f2.readlines()
temp1 = [line[:-1].split() for line in lines1]
temp2 = [line[:-1].split() for line in lines2]


f1.close()
f2.close()

G = nx.Graph()

print 'loading edges'

for i in temp1:
    G.add_edge(i[0],i[1])

print 'loading nodes'
for i in temp2:
    G.add_node(i[0])


print 'calc core number'
cn = nx.core_number(G)
print 'calc clique number'
kn  = nx.node_clique_number(G)
print 'calc number of cliques'
kn2 = nx.number_of_cliques(G)
print 'calculating node  measures'
for u,e in G.nodes_iter(data=True):

    dr = str(nx.degree(G,u))
    cl = str(nx.clustering(G,u))
    #cc = str(nx.betweenness_centrality(G)[u])
    ct = cn[u]
    knu = kn[u]
    kn2u = kn2[u]
    #ks = str(nx.k_shell(G)[u])
    f3.write(u+" "+dr+" "+cl+" "+str(ct)+" "+str(knu)+" "+str(kn2u)+"\n")
f3.close()

print 'calc edge measures'
for u,v,e in G.edges_iter(data=True):
    drp  = str(nx.degree(G,u) * nx.degree(G,v))
    f4.write(u+" "+v+" "+drp+"\n")

f4.close()



