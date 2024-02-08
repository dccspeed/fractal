from pyspark import SparkConf, SparkContext
from fractal import FractalContext
import os, sys, time
import networkx as nx
import numpy as np
import faiss
import karateclub as kc

path = sys.argv[1]
k = int(sys.argv[2])

print(path, k)

# create a SparkConf and SparkContext
conf = SparkConf().setAppName("MyFractalApp")
sc = SparkContext(conf=conf)

# fractal python api
sc.addPyFile("%s/fractal-core/src/main/python/fractal.py" % os.environ['FRACTAL_HOME'])
sc.addPyFile("%s/fractal-core/src/main/python/hexserializer.py" % os.environ[
    'FRACTAL_HOME'])

# filter function
def isclique(s):
    n = s.num_vertices
    for u in s.adjlists:
        if len(s.adjlists[u]) != n - 1:
            return False
    return True

# motif counting
fc = FractalContext(sc)
fg = fc.vertexLabeledGraphFromAdjLists(path)
#fg = fc.unlabeledGraphFromAdjLists(path)
frac = fg.inducedSubgraphsSample(k, 0.1)
#frac = fg.vfractoid().extend(2)
#for i in range(k-2):
#    frac = frac.extend(1)#.filter(isclique)


frac = frac.subgraphs()

def create_graph(s):
    g = nx.from_edgelist(s.pedges)
    for u in range(s.num_vertices):
        g.nodes[u]['feature'] = [s.pvlabels[u], len(s.adjlists[u])]
        #g.nodes[u]['vid'] = s.vids[u]
    return g
frac = frac.map(create_graph)

print("frac:", frac)

start = time.time()
subgraphs = frac.collect()
#model = kc.FeatherGraph()
model = kc.Graph2Vec(attributed=True)
model.fit([s.copy() for s in subgraphs])
X = model.get_embedding()
elapsed = time.time() - start

print("count:", len(subgraphs), "elapsed:", elapsed)
unique_rows = np.unique(X, axis=0)
print(unique_rows.shape, unique_rows)

querygraph = nx.from_edgelist([(0,1),(0,2),(0,3),(1,2),(2,3)])
querygraph.nodes[0]['feature'] = [1, 3]
querygraph.nodes[1]['feature'] = [4, 2]
querygraph.nodes[2]['feature'] = [4, 3]
querygraph.nodes[3]['feature'] = [0, 2]
querysubgraphs = [querygraph]
queries = model.infer([s.copy() for s in querysubgraphs])

database = X
index = faiss.IndexFlatL2(unique_rows.shape[1])   # build the index
print(index.is_trained)
index.add(database)                  # add vectors to the index
print(index.ntotal)

k = 10                          # we want to see 4 nearest neighbors
distances, neighbors = index.search(queries, k) # sanity check

def strgraph(g):
    return "[%s,%s]" % (g.edges, g.nodes.data())

for i in range(len(queries)):
    print("query:", i, "subgraph", strgraph(querysubgraphs[i]))
    for j in range(k):
        nid = neighbors[i][j]
        distance = distances[i][j]
        print("\tdatabase", nid,
              "subgraph:", strgraph(subgraphs[nid]), "distance:", distance)

# stop the context
fc.stop()
sc.stop()