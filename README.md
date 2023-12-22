# Fractal: A General-Purpose Graph Pattern Mining System
*Current Version:* SPARK-3.5.0

Fractal is a high performance and high productivity system for supporting distributed graph
pattern mining (GPM) applications. Our current version is tested on Spark 3.5.0.
Fractal features include:
* Interactive and intuitive API specifically designed for Graph Pattern Mining.
* Scalable and efficient.
* Efficient integration with RDD abstraction.

Fractal is open-source with the Apache 2.0 license.

## Research papers

* [v1.0.0](https://github.com/dccspeed/fractal/releases/tag/v1.0.0) : Fractal: A General-Purpose Graph Pattern Mining System ([SIGMOD '19](https://dl.acm.org/citation.cfm?id=3319875)).
* [v2.0.0](https://github.com/dccspeed/fractal/releases/tag/v2.0.0) : Graph Pattern Mining: Consolidation and Renewed Bearing ([HiPC '23](https://)).
* [v3.0.0](https://github.com/dccspeed/fractal): Current version: support for Spark 3.0 and Java 11.

## Requirements for running

* OpenJDK 8 or 11
* Spark 3.5.0

## Preparing your input
Fractal currently takes as input undirected labeled graph stored in a 
directory:

* (mandatory) ```graph/metadata```: single line containing the number of 
vertices (```n```) and the number edges (```m```) in the graph separated by 
a single space.
* (mandatory) ```graph/adjlists```: each line ```u = 0..n-1``` holds the 
adjacency list of vertex ```u```. Each item in this list is a pair
```(v,e)``` representing respectively, neighbor vertex ```v``` and edge id ```e```. 
Edge ids are also represented as indexes ```e = 0..m-1```
* (optional) ```graph/vlabels```: line ```i``` holds the label of vertex 
```i```.
* (optional) ```graph/elabels```: line ```i``` holds the label of edge
  ```i```.

Example: directory ```data/citeseer``` illustrates a valid formatting.

## Quick start with interactive notebook via Docker (local)

Run the following command to build a local Docker image that runs an Almond Scala/Spark Kernel Notebook with support
for Fractal:

```
docker buildx build --output type=docker --tag fractalnb -f notebook/Dockerfile https://github.com/dccspeed/fractal.git
```

Run the container:

```
docker run -it --rm -p 8888:8888 fractalnb:latest
```

The local URL for accessing the notebook kernel should appear in the output.
Notebook examples are provided in ```notebook/```

## Quick installation via Docker (local)

We provide a Docker image for this project. Run the following command to build a local Docker image:
```
docker buildx build --output type=docker --tag fractal https://github.com/dccspeed/fractal.git
```

### Running built-in applications

For a list and description regarding built-in applications:

```
docker run fractal:latest
```

Data folder with input graphs can be mounted via Docker volumes (```-v```). Arguments to Fractal runner are passed via 
Docker environment variables (```-e```). For example, the following command submit a Pattern-oblivious motif counting
application as a Docker container:

```
docker run -v ./data/:/data -e app=motifs_po -e steps=3 -e inputgraph=/data/citeseer fractal:latest
```

## Manual installation (distributed)

1. Download and configure Spark 3.5.0:

```
export JAVA_HOME=<openjdk-8-installation-folder>
wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3-scala2.13.tgz
tar xf spark-3.5.0-bin-hadoop3-scala2.13.tgz
mv spark-3.5.0-bin-hadoop3-scala2.13 spark
cd spark
export SPARK_HOME=`pwd` 
```

2. Clone and build Fractal:
```
git clone https://github.com/dccspeed/fractal.git # or direct download
cd fractal
export FRACTAL_HOME=`pwd`
./gradlew jar # download dependencies and build the project
./gradlew test # run tests
```

### Running built-in applications

For a list and description regarding built-in applications:

```$xslt
./bin/fractal.sh
```

### Running custom applications

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
