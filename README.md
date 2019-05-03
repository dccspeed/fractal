# Fractal: A General-Purpose Graph Pattern Mining System

*Current Version:* SPARK-2.2.0

Fractal is a high performance and high productivity system for supporting distributed graph
pattern mining (GPM) applications. Our current version is built on top of Spark 2.x.x.
Fractal features include:
* Interactive and intuitive API specifically designed for Graph Pattern Mining.
* Scalable and efficient.

Fractal is open-source with the Apache 2.0 license.

## Requirements for running

* OpenJDK 8+
* Spark 2.x.x

## Preparing your input
Fractal currently takes as input graphs with the following format:

```
# <num vertices> <num edges>
<vertex id> <vertex label> [<neighbour id1> <neighbour id2> ... <neighbour id n>]
<vertex id> <vertex label> [<neighbour id1> <neighbour id2> ... <neighbour id n>]
...
```

Vertex ids are expected to be sequential integers between 0 and (total number of vertices - 1).

## Getting started

1. Clone and build Fractal:
```
git clone https://github.com/dccspeed/fractal.git
cd fractal
export FRACTAL_HOME=`pwd`
./gradlew assembly
```

2. Execute a batch example example:
```
steps=2 inputgraph=$FRACTAL_HOME/data/citeseer-single-label.graph alg=cliques ./bin/fractal.sh
```

3. Build your own application using Fractal API.
For example, counting motifs with three vertices:
```
object MotifsApp {
  def main(args: Array[String]): Unit = {
    // environment setup
    val conf = new SparkConf().setAppName("TriangleCount")
    val sc = new SparkContext(conf)
    val fc = new FractalContext(sc)
    val graphPath = "data/cube.graph"
    val fgraph = new FractalGraph(graphPath, fc)

    // motifs application
    val AGG_MOTIFS = "motifs"
    val motifs = fgraph.vfractoid.
      expand(1).
      aggregate [Pattern,LongWritable] (
        AGG_MOTIFS,
        (e,c,k) => { e.getPattern },
        (e,c,v) => { v.set(1); v },
        (v1,v2) => { v1.set(v1.get() + v2.get()); v1 }).
      explore(2)
    println("motifs = " + motifs.aggregationMap(AGG_MOTIFS))

    // environment cleaning
    fc.stop()
    sc.stop()
  }
}
```
 
4. Build your own application by adding ```build/libs/fractal-SPARK-2.2.0-all.jar```
to your project's classpath.

