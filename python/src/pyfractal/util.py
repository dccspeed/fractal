import mmap
import struct
import time

import networkx as nx
import torch

def networkx_from_string(sstr):
    g = nx.Graph()
    toks = iter(sstr.split(","))
    num_vertices = int(next(toks))
    num_edges = int(next(toks))
    edgeids = []
    vertexids = []
    edges = []
    pvlabels = []
    pelabels = []

    for i in range(num_vertices):
        vertexids.append(int(next(toks)))

    for i in range(num_edges):
        edgeids.append(int(next(toks)))

    for i in range(num_edges):
        src = int(next(toks))
        dst = int(next(toks))
        edges.append((src, dst))

    for i in range(num_vertices):
        pvlabels.append(int(next(toks)))

    for i in range(num_edges):
        pelabels.append(int(next(toks)))

    for i in range(num_vertices):
        g.add_node(vertexids[i], label=pvlabels[i])

    for i in range(num_edges):
        e = edges[i]
        g.add_edge(vertexids[e[0]], vertexids[e[1]], label=pelabels[i], id=edgeids[i])

    return g

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

def write_pyg_data_as_fractal_graph(data, graphdir):
    adjlists = dict()
    edgeidx = dict()
    nedges = 0
    for e in data.edge_index.t():
        u, v = e.tolist()
        adjlist = adjlists.get(u, [])
        if u < v:
            eid = nedges
            nedges += 1
            edgeidx[(u, v)] = eid
        else:
            eid = edgeidx[(v, u)]
        adjlist.append((v, eid))
        adjlists[u] = adjlist

    nvertices = data.x.shape[0]

    with open(f"{graphdir}/metadata", "w") as f:
        f.write(f"{nvertices} {nedges}\n")

    with open(f"{graphdir}/adjlists", "w") as f:
        for u in range(nvertices):
            adjlist = adjlists.get(u, [])
            for i in range(len(adjlist)):
                v, e = adjlist[i]
                if i > 0: f.write(" ")
                f.write(f"{v},{e}")
            f.write("\n")

def get_memory_mapped_2dlongtensor(file_path):
    # Open the memory-mapped file
    with open(file_path, mode="r+b") as file:
        # Memory-map the file
        mmapped = mmap.mmap(file.fileno(), 0)

        def get_status():
            status = struct.unpack(">i", mmapped[:4])[0]
            return status

        while get_status() == 0:
            time.sleep(100 * 10e-6)

        # read shape
        nrows = struct.unpack(">i", mmapped[4:8])[0]
        ncols = struct.unpack(">i", mmapped[8:12])[0]

        # read long values
        datasize = struct.calcsize(f">{nrows * ncols}q")
        data = struct.unpack(f">{nrows * ncols}q", mmapped[12:12+datasize])

        # convert to PyTorch tensor
        tensor = torch.tensor(data, dtype=torch.float32).view(nrows, ncols)

        # close the memory-mapped file
        mmapped.close()

        return tensor




