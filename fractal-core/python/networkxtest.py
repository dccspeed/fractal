from pyspark import SparkConf, SparkContext
from fractal import FractalContext
import networkx as nx
import os, sys

path = sys.argv[1]
k = int(sys.argv[2])
sample = float(sys.argv[3])

# create a SparkConf and SparkContext
conf = SparkConf().setAppName("MyFractalApp")
sc = SparkContext(conf=conf)

# fractal python api
#sc.addPyFile("%s/fractal-core/src/main/python/fractal.py" % os.environ[
## 'FRACTAL_HOME'])
#sc.addPyFile("%s/fractal-core/src/main/python/hexserializer.py" % os.en# viron[
#    'FRACTAL_HOME'])

# motif counting
fc = FractalContext(sc)
fg = fc.unlabeledGraphFromAdjLists(path)
frac = fg.induced_subgraphs_sample(k, sample)
subgraphs = frac.subgraphs_networkx().collect()

for s in subgraphs:
    print(nx.to_edgelist(s))
print("Number of subgraphs: %d" % len(subgraphs))

# stop the context
fc.stop()
sc.stop()