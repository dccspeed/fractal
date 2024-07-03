import matplotlib.pyplot as plt
import networkx as nx
from networkx.drawing.nx_pydot import graphviz_layout


def draw_enumeration_tree_from_subgraphs(subgraphs):
    allpaths = [list(s.nodes()) for s in subgraphs]
    nodes = set()
    for p in allpaths:
        for u in p:
            nodes.add(u)

    num_nodes = len(nodes)
    G = nx.DiGraph()
    for root in range(num_nodes):
        G.add_node((-1, -1), vid=-1)
        paths = [p for p in allpaths if p[0] == root]
        paths_with_prefix = []
        for p in paths:
            prefix = []
            ppfirst = (p[0], tuple(prefix))
            pp = [ppfirst]
            G.add_edge((-1, -1), ppfirst)
            for i in range(1, len(p)):
                prefix.append(p[i - 1])
                pp.append((p[i], tuple(prefix)))
                paths_with_prefix.append(pp)

        for p in paths_with_prefix:
            for u in p:
                G.add_node(u, vid=u[0])
            u = p[0]
            for i in range(1, len(p)):
                v = p[i]
                G.add_edge(u, v)
                u = v

    plt.figure(figsize=(20, 5))
    plt.title("Árvore de Enumeração")
    groups = set(nx.get_node_attributes(G, 'vid').values())
    nodes = G.nodes()
    colors = [G.nodes[n]['vid'] for n in nodes]
    labels = nx.get_node_attributes(G, 'vid')
    pos = graphviz_layout(G, prog="dot")
    nx.draw_networkx(G, pos, vmin=-1, vmax=max(groups), labels=labels, with_labels=True, node_color=colors, node_size=400, cmap=plt.cm.Pastel1)


def draw_fractal_graph(fg):
    num_vertices = fg.vfractoid().extend(1).count()

    wholegraph = fg \
        .vfractoid().extend(num_vertices).subgraphs_networkx().collect()[0]

    plt.figure(figsize=(20, 5))
    plt.title("Grafo de entrada")
    nodes = wholegraph.nodes()
    labels = {l: l for l in nodes}
    colors = [c for c in nodes]
    pos = graphviz_layout(wholegraph, prog="neato")
    nx.draw_networkx(wholegraph, pos, vmin=-1, vmax=max(colors), labels=labels, with_labels=True, node_color=colors, node_size=1000, cmap=plt.cm.Pastel1)
