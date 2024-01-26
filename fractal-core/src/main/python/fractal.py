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
        return Fractoid(self._sc, self._fracjvm.pythonFilter(filterstr))

    def subgraphs(self):
        subgraphs = self._fracjvm.pythonSubgraphs()
        subgraphs = self._sc._jvm.org.apache.spark.api.python.SerDeUtil.javaToPython(subgraphs)
        subgraphs = RDD(subgraphs, self._sc)
        subgraphs = subgraphs.map(lambda sstr: Subgraph(sstr))
        return subgraphs


class FractalGraph:
    def __init__(self, sc, fgjvm):
        self._sc = sc
        self._fgjvm = fgjvm

    def vfractoid(self):
       return Fractoid(self._sc, self._fgjvm.vfractoid())

    def efractoid(self):
        return Fractoid(self._sc, self._fgjvm.vfractoid())

    def pfractoid(self, pattern):
        raise NotImplementedError

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
        for i in range(self.num_vertices):
            self.vids.append(int(next(toks)))
        for i in range(self.num_edges):
            self.eids.append(int(next(toks)))
        for i in range(self.num_edges):
            src = int(next(toks))
            dst = int(next(toks))
            self.pedges.append((src,dst))
        for i in range(self.num_vertices):
            self.pvlabels.append(int(next(toks)))
        for i in range(self.num_edges):
            self.pelabels.append(int(next(toks)))

    def __str__(self):
        return "Subgraph(num_vertices=%d, num_edges=%d, vids=%s, eids=%s, " \
               "pedges=%s, pvlabels=%s, pelabels=%s)" % (
            self.num_vertices, self.num_edges, self.vids, self.eids,
            self.pedges, self.pvlabels, self.pelabels)
