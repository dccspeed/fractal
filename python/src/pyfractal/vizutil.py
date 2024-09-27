import matplotlib.pyplot as plt
import networkx as nx
from networkx.drawing.nx_agraph import graphviz_layout
from itertools import count


def draw_vertex_enumeration_tree_from_subgraphs(subgraphs, figsize, title=None, **kwargs):
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

    plt.figure(figsize=figsize)
    if title is not None:
        plt.title(title)
    groups = set(nx.get_node_attributes(G, 'vid').values())
    nodes = G.nodes()
    colors = [G.nodes[n]['vid'] for n in nodes]
    labels = nx.get_node_attributes(G, 'vid')
    pos = graphviz_layout(G, prog="dot")
    nx.draw(G, pos, vmin=-1, vmax=max(groups), labels=labels, with_labels=True, node_color=colors,
                     cmap=plt.cm.Pastel1, linewidths=1, edgecolors='black', **kwargs)


def draw_edge_enumeration_tree_from_subgraphs(subgraphs, figsize, title=None, **kwargs):
    allpaths = [[f"{e[0]}-{e[1]}" for e in s.edges()] for s in subgraphs]
    nodes = set()
    for p in allpaths:
        for u in p:
            nodes.add(u)

    node_to_id = {'-1-1': -1}
    id_to_node = {-1: '-1-1'}
    for n in nodes:
        i = len(node_to_id)
        node_to_id[n] = i
        id_to_node[i] = n

    num_nodes = len(nodes)
    G = nx.DiGraph()
    for root in sorted(nodes):
        G.add_node('-1-1', vid=-1)
        paths = [p for p in allpaths if p[0] == root]
        paths_with_prefix = []
        for p in paths:
            prefix = []
            ppfirst = (p[0], tuple(prefix))
            pp = [ppfirst]
            G.add_edge('-1-1', ppfirst)
            for i in range(1, len(p)):
                prefix.append(p[i - 1])
                pp.append((p[i], tuple(prefix)))
                paths_with_prefix.append(pp)

        for p in paths_with_prefix:
            for u in p:
                G.add_node(u, vid=node_to_id[u[0]])
            u = p[0]
            for i in range(1, len(p)):
                v = p[i]
                G.add_edge(u, v)
                u = v

    plt.figure(figsize=figsize)
    if title is not None:
        plt.title(title)
    groups = set(nx.get_node_attributes(G, 'vid').values())
    nodes = G.nodes()
    colors = [G.nodes[n]['vid'] for n in nodes]
    labels = {n: id_to_node[i] for n, i in nx.get_node_attributes(G, 'vid').items()}
    pos = graphviz_layout(G, prog="dot")
    nx.draw(G, pos, vmin=-1, vmax=max(groups), labels=labels, with_labels=True, node_color=colors,
            cmap=plt.cm.Pastel1, linewidths=1, edgecolors='black', **kwargs)


def draw_fractal_graph(fg, figsize, prog="sfdp", title=None, color_mode="vid", **kwargs):
    edge_subgraphs = fg.efractoid().extend(1).subgraphs_networkx().collect()
    wholegraph = nx.Graph()
    for eg in edge_subgraphs:
        wholegraph = nx.compose(wholegraph, eg)

    plt.figure(figsize=figsize)
    if title is not None:
        plt.title(title)
    nodes = wholegraph.nodes()
    labels = {l: l for l in nodes}
    if color_mode == "vid":
        colors = [c for c in nodes]
    elif color_mode == "vlabel":
        vlabels = nx.get_node_attributes(wholegraph, 'label')
        colors = [vlabels[c] for c in nodes]

    pos = graphviz_layout(wholegraph, prog=prog)
    nx.draw_networkx(wholegraph, pos, vmin=-1, vmax=max(colors), labels=labels, with_labels=True, node_color=colors,
                     cmap=plt.cm.Pastel1, linewidths=1, edgecolors='black', **kwargs)


def draw_graphs_in_grid(subgraphs_titles, ncols, figsize, pad_axis_x=1, pad_axis_y=1, with_labels=False, labeled=True, **kwargs):
    if len(subgraphs_titles) == 0:
        return None
    if len(subgraphs_titles) < ncols:
        ncols = len(subgraphs_titles)
    nrows = int(len(subgraphs_titles) / ncols)
    if len(subgraphs_titles) % ncols > 0:
        nrows += 1
    fig, axes = plt.subplots(nrows, ncols, figsize=figsize)
    idx = 0
    st_iterator = iter(subgraphs_titles)
    for i in range(nrows):
        for j in range(ncols):
            ax = axes[i, j] if nrows > 1 else axes[j] if ncols > 1 else axes
            try:
                subgraph = next(st_iterator)
            except:
                ax.set_visible(False)
                continue
            title = subgraphs_titles[subgraph]
            nodes = subgraph.nodes()
            vlabels = nx.get_node_attributes(subgraph, 'label')
            if labeled:
                colors = [vlabels[c] for c in nodes]
            else:
                colors = "white"
            pos = nx.circular_layout(subgraph)
            minx = min([x for x,_ in pos.values()])
            maxx = max([x for x,_ in pos.values()])
            ax.set_xlim(minx-pad_axis_x, maxx+pad_axis_x)
            miny = min([y for _,y in pos.values()])
            maxy = max([y for _,y in pos.values()])
            ax.set_ylim(miny-pad_axis_y, maxy+pad_axis_y)
            ax.set_title(title, fontsize=kwargs.get('font_size', 20))
            ax.axis('off')
            nx.draw(subgraph, pos, vmin=-1, with_labels=with_labels, ax=ax, node_color=colors, cmap=plt.cm.Pastel1,
                    linewidths=1, edgecolors='black', **kwargs)
            idx += 1

    return fig
