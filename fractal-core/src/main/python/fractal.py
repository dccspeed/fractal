from pyspark.rdd import RDD
import hexserializer as hexser

class Fractoid:
    def __init__(self, sc, fracjvm):
        self._sc = sc
        self._fracjvm = fracjvm

    def extend(self, k):
        return Fractoid(self._sc, self._fracjvm.extend(k))

    def filter(self, filter):
        filterstr = hexser.dumps(filter)
        return Fractoid(self._sc, self._fracjvm.pythonFilter2(filterstr))

    def subgraphs(self):
        subgraphs = self._fracjvm.pythonSubgraphs()
        subgraphs = self._sc._jvm.org.apache.spark.api.python.SerDeUtil.javaToPython(subgraphs)
        subgraphs = RDD(subgraphs, self._sc)
        subgraphs = subgraphs.map(lambda sstr: Subgraph(sstr))
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
        self._gmlib = sc._jvm.br.ufmg.cs.systems.fractal.gmlib \
            .BuiltInApplications(fgjvm)

    def vfractoid(self):
       return Fractoid(self._sc, self._fgjvm.vfractoid())

    def efractoid(self):
        return Fractoid(self._sc, self._fgjvm.vfractoid())

    def pfractoid(self, pattern):
        raise NotImplementedError

    def motifsPO(self, k):
        return self._gmlib.motifsPO(k).toJavaRDD()

    def inducedSubgraphsSample(self, k, fraction):
        return Fractoid(self._sc,
                        self._gmlib.inducedSubgraphsSample(k, fraction))

class FractalContext:
    def __init__(self, sc):
        self._sc = sc
        self._fcjvm = sc._jvm.br.ufmg.cs.systems.fractal.FractalContext(
            sc._jsc.sc(), "info")

    def unlabeledGraphFromAdjLists(self, path):
        return FractalGraph(self._sc,
                            self._fcjvm.unlabeledGraphFromAdjLists(path))

    def vertexLabeledGraphFromAdjLists(self, path):
        return FractalGraph(self._sc,
                            self._fcjvm.vertexLabeledGraphFromAdjLists(path))

    def vertexEdgeLabeledGraphFromAdjLists(self, path):
        return FractalGraph(self._sc,
                            self._fcjvm.vertexEdgeLabeledGraphFromAdjLists(path))

    def stop(self):
        self._fcjvm.stop()


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
            self.pedges.append((src,dst))
            add_edge(src, dst)
        for i in range(self.num_vertices):
            self.pvlabels.append(int(next(toks)))
        for i in range(self.num_edges):
            self.pelabels.append(int(next(toks)))

    def pattern(self):
        return self.pedges

    def edgelist(self):
        vertices = self.vids
        return [(vertices[src],vertices[dst]) for (src,dst) in self.pedges]

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "Subgraph(num_vertices=%d, num_edges=%d, vids=%s, eids=%s, " \
               "pedges=%s, pvlabels=%s, pelabels=%s)" % (
            self.num_vertices, self.num_edges, self.vids, self.eids,
            self.pedges, self.pvlabels, self.pelabels)
