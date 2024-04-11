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

# motif counting
fc = FractalContext(sc)
fg = fc.unlabeledGraphFromAdjLists(path)
frac = fg.vfractoid().extend(k)
subgraphs = frac.subgraphs()
subgraphs = subgraphs.map(lambda s: (frozenset(s.pedges), 1))
def sum(v1,v2): return v1 + v2
patterncount = subgraphs.reduceByKey(sum).collect()

for p,c in patterncount:
    print(p, c)

# stop the context
fc.stop()
sc.stop()