from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,\
    MapType
from fractal import FractalContext
import os, sys, time
import networkx as nx
import numpy as np
import faiss
import karateclub as kc
import json

path = sys.argv[1]
k = int(sys.argv[2])

print(path, k)

# create a SparkConf and SparkContext
spark = SparkSession.builder \
    .appName('SparkExample') \
    .getOrCreate()
sc = spark.sparkContext

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
frac = fg.inducedSubgraphsSample(k, 0.5)
#frac = frac.filter(isclique)
#frac = fg.vfractoid().extend(2)
#for i in range(k-2):
#    frac = frac.extend(1)#.filter(isclique)


frac = frac.jsonsubgraphs().map(lambda x: json.loads(x))
print(frac.collect()[0])

schema = StructType([
    StructField('numVertices', IntegerType(), True),
    StructField('numEdges', IntegerType(), True),
    StructField('v1', StructType([StructField('id', IntegerType(), True), StructField('label', StringType(), True)]), True),
    StructField('v2', StructType([StructField('id', IntegerType(), True),
                                  StructField('label', IntegerType(), True)]), True),
    StructField('v3', StructType([StructField('id', IntegerType(), True),
                                  StructField('label', IntegerType(), True)]), True),
    StructField('e1', StructType([StructField('src', IntegerType(), True),
                                  StructField('dst', IntegerType(), True),
                                    StructField('id', IntegerType(), True),
                                    StructField('label', IntegerType(),
                                                True)]), True),
    StructField('e2', StructType([StructField('src', IntegerType(), True),
                                  StructField('dst', IntegerType(), True),
                                  StructField('id', IntegerType(), True),
                                  StructField('label', IntegerType(),
                                              True)]), True),
    StructField('e3', StructType([StructField('src', IntegerType(), True),
                                  StructField('dst', IntegerType(), True),
                                  StructField('id', IntegerType(), True),
                                  StructField('label', IntegerType(),
                                              True)]), True)

])

fracdf = spark.createDataFrame(frac, schema = schema)
fracdf.printSchema()
fracdf.show()
fracdf.select(fracdf['v1.id'],fracdf['v1.label']).show()

print("frac:", frac)

start = time.time()
subgraphs = frac.collect()
elapsed = time.time() - start

print("elapsed:", elapsed)
print("subgraphs:", subgraphs[:2])

# stop the context
fc.stop()
spark.stop()