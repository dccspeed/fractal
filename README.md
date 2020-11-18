# Fractal: A General-Purpose Graph Pattern Mining System
[![Build Status](https://travis-ci.com/dccspeed/fractal.svg?branch=master)](https://travis-ci.com/dccspeed/fractal)

*Current Version:* SPARK-2.2.0

Fractal is a high performance and high productivity system for supporting distributed graph
pattern mining (GPM) applications. Our current version is built on top of Spark 2.x.x.
Fractal features include:
* Interactive and intuitive API specifically designed for Graph Pattern Mining.
* Scalable and efficient.
* Efficient integration with RDD abstraction.

Fractal is open-source with the Apache 2.0 license. Fractal paper is available [here](https://dl.acm.org/citation.cfm?id=3319875).

## Requirements for running

* OpenJDK 8
* Spark 2.x.x

## Preparing your input
Fractal currently takes as input undirected multi-labeled graph.

* Vertices are numbered from ```0``` until ```nvertices - 1```.
* Edges are numbered from ```0``` until ```nedges - 1```.

The following adjacency lists format is used:
```
<nvertices> <nedges>
<vertex data>( <neighbor data>)*
<vertex data>( <neighbor data>)*
...
```

Where:

```
<vertex data> := <vlabel>(,<vlabel>)*

<neighbor data> := <vid>,<eid>(,<elabel>)*

<nvertices> := <nedges> := <vlabel> := <elabel> := [0-9][0-9]* // 32 bit integer
<vid> := [0-9][0-9]* // 32 bit integer within range [0,nvertices)
<eid> := [0-9][0-9]* // 32 bit integer within range [0,nedges)
```

Directory ```data/```  includes a few examples (extension ```*.sc```).

## Installing Fractal

1. Download and configure Spark 2.x.x:

```
export JAVA_HOME=<openjdk-8-installation-folder>
cd <repositories-folder>
wget https://archive.apache.org/dist/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz
mv spark-2.4.3-bin-hadoop2.7.tgz spark
cd spark
export SPARK_HOME=`pwd` 
```

2. Clone and build Fractal:
```
cd <repositories-folder>
git clone https://github.com/dccspeed/fractal.git
cd fractal
export FRACTAL_HOME=`pwd`
./gradlew jar # alternatively, run './gradlew test jar' for tests
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
the dataset ```data/citeseer-single-label.sc```:

```
args=data/citeseer-single-label.sc app_class=br.ufmg.cs.systems.fractal.apps.MyMotifsApp ./bin/fractal-custom-app.sh
```
