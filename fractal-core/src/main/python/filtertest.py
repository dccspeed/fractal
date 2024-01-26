from pyspark import SparkConf, SparkContext
from fractal import FractalContext
import os, sys

path = sys.argv[1]
k = int(sys.argv[2])

# create a SparkConf and SparkContext
conf = SparkConf().setAppName("MyFractalApp")
sc = SparkContext(conf=conf)

# fractal python api
sc.addPyFile("%s/fractal-core/src/main/python/fractal.py" % os.environ['FRACTAL_HOME'])
sc.addPyFile("%s/fractal-core/src/main/python/hexserializer.py" % os.environ[
    'FRACTAL_HOME'])

# motif counting
fc = FractalContext(sc)
fg = fc.unlabeledGraphFromAdjLists(path)
func = lambda s: s.num_edges == 6
frac = fg.vfractoid().extend(k).filter(func)
subgraphs = frac.subgraphs().collect()

for subgraph in subgraphs:
    print(subgraph)

# stop the context
fc.stop()
sc.stop()