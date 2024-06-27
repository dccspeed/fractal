import sys
import pyfractal
from pyfractal.model import FractalContext
from torch_geometric.utils import from_networkx

graphdir = sys.argv[1]
k = int(sys.argv[2])

spark = pyfractal.DefaultSparkBuilder \
    .master("local[8]") \
    .config("spark.driver.memory", "2g") \
    .appName("demo") \
    .getOrCreate()

fc = FractalContext(spark)
fg = fc.unlabeledGraphFromAdjLists(graphdir)
networkx_subgraphs = fg.khop_induced_subgraphs(k).collect()
for subgraph in networkx_subgraphs:
    pyg_graph = from_networkx(subgraph)
    print(pyg_graph)

fc.stop()
spark.stop()
