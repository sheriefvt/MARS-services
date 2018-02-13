__author__ = 'Sherif'



import networkx as nx
import sys



name = sys.argv[1]

f1 = open('{n}.uel'.format(n=name), "r")
f2 = open('{n}2.nodes'.format(n=name), "w")
f3 = open('{n}2.uel'.format(n=name), "w")
f4 = open('{n}2.del'.format(n=name), "w")

lines1 = f1.readlines()

temp1 = [line[:-1].split() for line in lines1]


f1.close()

G = nx.Graph()

print 'loading edges'

for i in temp1:
    G.add_edge(i[0],i[1])

G2 = G.to_undirected()
G3 = G.to_directed()

for n  in G2.nodes_iter(data=False):

          f2.write(str(n)+"\n")


for u,v  in G2.edges_iter(data=False):
         f3.write(str(u)+" "+str(v)+"\n")

for u,v  in G3.edges_iter(data=False):
         f4.write(str(u)+" "+str(v)+"\n")

f2.close()
f3.close()
f4.close()





