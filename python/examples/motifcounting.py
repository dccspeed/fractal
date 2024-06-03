from pyspark.sql import SparkSession
import pyfractal as pf
from pyfractal.model import FractalContext
import os, sys

path = sys.argv[1]
k = int(sys.argv[2])

fractal_jar_path = pf.fractal_jar_path()

print(fractal_jar_path)

# create a SparkConf and SparkContext
spark = SparkSession.builder\
    .master("local")\
    .config("spark.driver.memory", "2g")\
    .config("spark.jars.packages", "com.koloboke:koloboke-impl-jdk8:1.0.0,com.typesafe.akka:akka-remote_2.13:2.5.23")\
    .config("spark.jars", fractal_jar_path)\
    .appName("demo")\
    .getOrCreate()

# fractal python api
#sc.addPyFile("%s/fractal-core/src/main/python/fractal.py" % os.environ['FRACTAL_HOME'])

# motif counting
fc = FractalContext(spark.sparkContext)
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
spark.stop()