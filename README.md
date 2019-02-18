# Fractal: Efficient and Interactive Graph Pattern Mining System

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

2. Execute a built-in example:
```
steps=2 inputgraph=$FRACTAL_HOME/data/citeseer-single-label.graph alg=cliques ./bin/fractal.sh
```

