# Fractal: A General-Purpose Graph Pattern Mining System
[![Build Status](https://travis-ci.com/dccspeed/fractal.svg?branch=master)](https://travis-ci.com/dccspeed/fractal)

*Current Version:* SPARK-2.4.0

Fractal is a high performance and high productivity system for supporting distributed graph
pattern mining (GPM) applications. Our current version is was tested Spark 2.
4.0.
Fractal features include:
* Interactive and intuitive API specifically designed for Graph Pattern Mining.
* Scalable and efficient.
* Efficient integration with RDD abstraction.

Fractal is open-source with the Apache 2.0 license. Fractal paper is available [here](https://dl.acm.org/citation.cfm?id=3319875).

## Requirements for running

* OpenJDK 8
* Spark 2.4.0

## Preparing your input
Fractal currently takes as input undirected labeled graph stored in a 
directory with three files:

* (mandatory) ```graph/metadata```: single line containing the number of 
vertices (```n```) and the number edges (```m```) in the graph separated by 
a single space.
* (mandatory) ```graph/adjlists```: each line ```i = 0..n-1``` holds the 
adjacency list of vertex ```i```. Each item in this list is a pair
```(u,e)``` representing respectively, neighbor vertex u and edge id e.
* (optional) ```graph/vlabels```: line ```i``` holds the label of vertex 
```i```.
* (optional) ```graph/elabels```: line ```i``` holds the label of edge
  ```i```.

Example: directory ```data/citeseer``` illustrates a valid formatting.

## Installing Fractal

1. Download and configure Spark 2.4.3:

```
export JAVA_HOME=<openjdk-8-installation-folder>
cd <repositories-folder>
wget https://archive.apache.org/dist/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz
mv spark-2.4.0-bin-hadoop2.7.tgz spark
cd spark
export SPARK_HOME=`pwd` 
```

2. Clone and build Fractal:
```
git clone https://github.com/dccspeed/fractal.git # or direct download
cd fractal
export FRACTAL_HOME=`pwd`
./gradlew jar # download dependencies and build the project
```

## Running built-in applications

For a list and description regarding built-in applications:

```$xslt
bin/fractal.sh
```

## Running custom applications

You can also implement your own application using Fractal API. We provide the subproject 
"fractal-apps" to make this process easier. All you need to do is to add your application class
into ```fractal-apps/src/```, re-compile the project with ```./gradlew jar```, and run your
code with the ```bin/fractal-custom-app.sh``` script:

```
./bin/fractal-custom-app.sh
```

Please, refer to
```fractal-apps/src/main/scala/br/ufmg/cs/systems/fractal/apps/```
for an example.

Next, we re-compile the project with ```./gradlew jar``` and run the
 application over
the dataset ```data/citeseer```:

```
args=data/citeseer app_class=br.ufmg.cs.systems.fractal.apps.MyMotifsApp ./bin/fractal-custom-app.sh
```

## External software acknowledgements

The following open-source projects are used in Fractal:

- [Bliss](http://www.tcs.hut.fi/Software/bliss/)
- [AsyncProfiler](https://github.com/jvm-profiling-tools/async-profiler)
