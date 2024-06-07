import networkx as nx
def pattern_to_networkx(jvmpattern):
    g = nx.Graph()
    nedges = jvmpattern.getNumberOfEdges()
    edges = jvmpattern.getEdges()
    for i in range(nedges):
        edge = edges.get(i)
        src = edge.getSrcPos()
        dst = edge.getDestPos()
        g.add_edge(src, dst)
    return g



