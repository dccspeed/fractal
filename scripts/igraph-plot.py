import igraph as ig
import sys
import random as rd

# edges=[(0,1),(1,2),(1,3),(1,4),(1,5)],vlabels=[1,1,1,1,1,1],elabels=[1,1,1,1,1]

pattern_str = sys.argv[1]
g = ig.Graph()

nvertices = 0
edges = []
vlabels = []
elabels = []

# edges and vertices
edges_str = pattern_str.split(",vlabels")[0]
edges_str = edges_str.split("edges=")[1]
edges_str = edges_str.split("[")[1].split("]")[0]
edges_str = edges_str[1:-1]
for edge_str in edges_str.split("),("):
    e = [int(u) for u in edge_str.split(",")]
    if e[0] + 1 > nvertices: nvertices = e[0] + 1
    if e[1] + 1 > nvertices: nvertices = e[1] + 1
    edges.append(e)

# vlabels
vlabels_str = pattern_str.split(",vlabels=")[1].split(",elabels=")[0]
vlabels_str = vlabels_str[1:-1]
vlabels = [int(l) for l in vlabels_str.split(",")]

# elabels
elabels_str = pattern_str.split(",elabels=")[1]
elabels_str = elabels_str[1:-1]
elabels = [int(l) for l in elabels_str.split(",")]

for u in range(nvertices):
    print("vertex", u, "label", vlabels[u])

for e in range(len(edges)):
    print("edge", edges[e], "label", elabels[e])

g.add_vertices(nvertices)
g.add_edges(edges)
colors = list(ig.drawing.colors.known_colors.keys())

g.vs["label"] = vlabels
g.es["label"] = elabels
rd.seed(0)
rd.shuffle(colors)
g.vs["color"] = [colors[vlabels[i]] for i in range(nvertices)]
rd.shuffle(colors)
g.es["color"] = [colors[elabels[i]] for i in range(len(elabels))]
ig.plot(g)
