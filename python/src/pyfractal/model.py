import json
import os
import tempfile
import threading
import time

import networkx as nx
from pyspark.rdd import RDD

import pyfractal.hexserializer as hexser
from pyfractal.util import pattern_to_networkx, get_memory_mapped_2dlongtensor, write_pyg_data_as_fractal_graph, \
    networkx_from_string


class Fractoid:
    def __init__(self, sc, fracjvm):
        self._sc = sc
        self._fracjvm = fracjvm

    def extend(self, k):
        return Fractoid(self._sc, self._fracjvm.extend(k))

    def filter(self, filter):
        filterstr = hexser.dumps(filter)
        return Fractoid(self._sc, self._fracjvm.pythonFilter(filterstr))

    def subgraphs(self):
        subgraphs = self._fracjvm.pythonSubgraphs()
        subgraphs = self._sc._jvm.org.apache.spark.api.python.SerDeUtil.javaToPython(subgraphs)
        subgraphs = RDD(subgraphs, self._sc)
        subgraphs = subgraphs.map(lambda sstr: Subgraph(sstr))
        return subgraphs

    def subgraphs_networkx(self):
        subgraphs = self._fracjvm.pythonSubgraphs()
        subgraphs = self._sc._jvm.org.apache.spark.api.python.SerDeUtil.javaToPython(subgraphs)
        subgraphs = RDD(subgraphs, self._sc)
        subgraphs = subgraphs.map(networkx_from_string)
        return subgraphs

    def jsonsubgraphs(self):
        subgraphs = self._fracjvm.jsonSubgraphs()
        subgraphs = self._sc._jvm.org.apache.spark.api.python.SerDeUtil.javaToPython(subgraphs)
        subgraphs = RDD(subgraphs, self._sc)
        return subgraphs

    def count(self):
        return self._fracjvm.aggregationCount()

    def __str__(self):
        return self._fracjvm.toString()


class FractalGraph:
    def __init__(self, sc, fgjvm):
        self._sc = sc
        self._fgjvm = fgjvm
        self._jvm = sc._jvm
        self._gmlib = sc._jvm.br.ufmg.cs.systems.fractal.gmlib \
            .BuiltInApplications(fgjvm)
        self._num_vertices = None
        self._num_edges = None

    def get_num_vertices(self):
        if self._num_vertices is None:
            self._num_vertices = self.vertex_induced().extend(1).count()
        return self._num_vertices

    def get_num_edges(self):
        if self._num_edges is None:
            self._num_edges = self.edge_induced().extend(1).count()
        return self._num_edges

    def set(self, key, value):
        return FractalGraph(self._sc, self._fgjvm.set(key, value))

    def vertex_induced(self):
        return Fractoid(self._sc, self._fgjvm.vfractoid())

    def edge_induced(self):
        return Fractoid(self._sc, self._fgjvm.efractoid())

    def pattern_induced(self, nxgraph, vertex_labeled=False, edge_labeled=False, induced=False):
        g = nxgraph.copy()
        for u in g.nodes:
            g.nodes[u]['label'] = g.nodes[u].get('label', 1)
        for u, v in g.edges:
            g.edges[u, v]['label'] = g.edges[u, v].get('label', 0)
        nxdata = json.dumps(nx.node_link_data(g))
        ser_pattern_obj = self._jvm.br.ufmg.cs.systems.fractal.pattern.SerializablePattern
        ser_pattern = ser_pattern_obj.fromNodeLinkNetworkxJSON(nxdata)
        pattern_utils = self._jvm.br.ufmg.cs.systems.fractal.pattern.PatternUtils
        pattern = pattern_utils.fromSerializablePattern(ser_pattern, vertex_labeled, edge_labeled, induced)
        return Fractoid(self._sc, self._fgjvm.pfractoid(pattern))

    def motif_counting(self, k):
        motif_count = self.motifsPO(k).collect()
        output = []
        for mc in motif_count:
            pattern_jvm = mc._1()
            pattern = pattern_to_networkx(pattern_jvm)
            count = mc._2()
            output.append((pattern, count))
        return output

    def pattern_querying(self, nxgraph, vertex_labeled=False, edge_labeled=False, induced=False):
        num_vertices = nxgraph.number_of_nodes()
        return self.pattern_induced(nxgraph, vertex_labeled, edge_labeled, induced).extend(num_vertices).subgraphs_networkx()

    def cliques(self, k):
        return Fractoid(self._sc, self._gmlib.cliquesPO(k)).subgraphs_networkx()

    def quasi_cliques(self, k, min_density):
        return Fractoid(self._sc, self._gmlib.quasiCliquesPO(k, min_density)).subgraphs_networkx()

    def frequent_subgraph_mining(self, k, min_support):
        min_image_support = min_support * self.get_num_vertices()
        if min_image_support - int(min_image_support) > 0:
            min_image_support += 1
        min_image_support = int(min_image_support)
        pattern_support = self._gmlib.fsmPO(min_image_support, k).toJavaRDD().collect()
        output = []
        for ps in pattern_support:
            pattern_jvm = ps._1()
            pattern = pattern_to_networkx(pattern_jvm)
            output.append(pattern)
        return output

    def graphlet_degree_vectors(self, k):
        with tempfile.NamedTemporaryFile() as tmpfile:
            path = tmpfile.name
            self._gmlib.graphletDegreeVectorsAsTensor(k, path)
            tensor = get_memory_mapped_2dlongtensor(path)
        return tensor

    def khop_induced_subgraphs(self, k):
        return Fractoid(self._sc, self._gmlib.kHopInducedSubgraphs(k)).subgraphs_networkx()

    def motifsPO(self, k):
        return self._gmlib.motifsPO(k).toJavaRDD()

    def induced_subgraphs(self, k):
        return self.vertex_induced().extend(k)

    def induced_subgraphs_sample(self, k, fraction):
        return Fractoid(self._sc,
                        self._gmlib.inducedSubgraphsSample(k, fraction))


class FractalContext:
    def __init__(self, sc):
        self.graphdir = None
        if sc.sparkContext is not None:
            sc = sc.sparkContext
        self._sc = sc
        self._fcjvm = sc._jvm.br.ufmg.cs.systems.fractal.FractalContext(
            sc._jsc.sc(), "error")
        script_path = os.path.dirname(os.path.realpath(__file__))
        sc.addPyFile("%s/model.py" % script_path)
        sc.addPyFile("%s/hexserializer.py" % script_path)
        def periodic_gc():
            self._sc._jvm.System.gc()
            threading.Timer(10, periodic_gc).start()

        periodic_gc()

    def unlabeledGraphFromPyGData(self, data):
        self.graphdir = tempfile.TemporaryDirectory(prefix="pydata2fractal", delete=False)
        write_pyg_data_as_fractal_graph(data, self.graphdir.name)
        return self.unlabeledGraphFromAdjLists(self.graphdir.name)

    def unlabeled_graph(self, path):
        return FractalGraph(self._sc,
                            self._fcjvm.unlabeledGraphFromAdjLists(path)).set("ws_external", "false")

    def vertex_labeled_graph(self, path):
        return FractalGraph(self._sc,
                            self._fcjvm.vertexLabeledGraphFromAdjLists(path)).set("ws_external", "false")

    def vertex_edge_labeled_graph(self, path):
        return FractalGraph(self._sc,
                            self._fcjvm.vertexEdgeLabeledGraphFromAdjLists(path)).set("ws_external", "false")

    def stop(self):
        self._fcjvm.stop()

    def __del__(self):
        if self.graphdir is not None:
            self.graphdir.cleanup()


class Subgraph:
    def __init__(self, sstr):
        toks = iter(sstr.split(","))
        self.num_vertices = int(next(toks))
        self.num_edges = int(next(toks))
        self.vids = []
        self.eids = []
        self.pedges = []
        self.pvlabels = []
        self.pelabels = []
        self.adjlists = {}

        def add_edge(src, dst):
            if src not in self.adjlists:
                self.adjlists[src] = set()
            self.adjlists[src].add(dst)
            if dst not in self.adjlists:
                self.adjlists[dst] = set()
            self.adjlists[dst].add(src)

        for i in range(self.num_vertices):
            self.vids.append(int(next(toks)))
        for i in range(self.num_edges):
            self.eids.append(int(next(toks)))
        for i in range(self.num_edges):
            src = int(next(toks))
            dst = int(next(toks))
            self.pedges.append((src, dst))
            add_edge(src, dst)
        for i in range(self.num_vertices):
            self.pvlabels.append(int(next(toks)))
        for i in range(self.num_edges):
            self.pelabels.append(int(next(toks)))

    def pattern(self):
        return self.pedges

    def to_networkx(self):
        edges = [(self.vids[src], self.vids[dst]) for (src, dst) in self.pedges]
        g = nx.from_edgelist(edges)
        for i in range(self.num_vertices):
            u = self.vids[i]
            g.nodes[u]['label'] = self.pvlabels[i]
        for i in range(self.num_edges):
            e = self.pedges[i]
            g[e[0]][e[1]]['label'] = self.pelabels[i]

        return g

    def edgelist(self):
        vertices = self.vids
        return [(vertices[src], vertices[dst]) for (src, dst) in self.pedges]

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "Subgraph(num_vertices=%d, num_edges=%d, vids=%s, eids=%s, " \
               "pedges=%s, pvlabels=%s, pelabels=%s)" % (
            self.num_vertices, self.num_edges, self.vids, self.eids,
            self.pedges, self.pvlabels, self.pelabels)
