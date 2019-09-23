# Fractal: A General-Purpose Graph Pattern Mining System
[![Build Status](https://travis-ci.com/dccspeed/fractal.svg?branch=master)](https://travis-ci.com/dccspeed/fractal)

*Current Version:* SPARK-2.2.0

Fractal is a high performance and high productivity system for supporting distributed graph
pattern mining (GPM) applications. Our current version is built on top of Spark 2.x.x.
Fractal features include:
* Interactive and intuitive API specifically designed for Graph Pattern Mining.
* Scalable and efficient.

Fractal is open-source with the Apache 2.0 license. Fractal paper is available [here](https://dl.acm.org/citation.cfm?id=3319875).

## Requirements for running

* OpenJDK 8
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

## Installing Fractal

1. Download and configure Spark 2.x.x:

```
export JAVA_HOME=<openjdk-8-installation-folder>
cd <repositories-folder>
wget https://archive.apache.org/dist/spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz
mv spark-2.2.0-bin-hadoop2.7.tgz spark
cd spark
export SPARK_HOME=`pwd` 
```

2. Clone and build Fractal:
```
cd <repositories-folder>
git clone https://github.com/dccspeed/fractal.git
cd fractal
export FRACTAL_HOME=`pwd`
./gradlew assemble
```

## Running built-in applications

Fractal includes the following built-in applications (GPM kernels):

- Motifs Enumeration & Counting
- Cliques Enumeration & Counting
- Frequent Subgraph Mining (FSM)
- Subgraph Querying

Please check out our [Fractal paper](https://dl.acm.org/citation.cfm?id=3319875) for more details on
those kernels. You can run those applications through the ```bin/fractal.sh``` script:

```
Description: Script launcher for Fractal built-in applications

info: FRACTAL_HOME is set to ...
info: SPARK_HOME is set to ...
error: app is unset

Usage:
app=fsm|motifs|cliques|cliquesopt|gquerying|gqueryingnaive|kws [OPTION]... [ALGOPTION]... fractal.sh

OPTION:
   master_memory=512m|1g|2g|...            'Master memory'                      Default: 2g
   num_workers=1|2|3|...                   'Number of workers'                  Default: 1
   worker_cores=1|2|3|...                  'Number of cores per worker'         Default: 1
   worker_memory=512m|1g|2g|...            'Workers memory'                     Default: 2g
   input_format=al|el                      'al: adjacency list; el: edge list'  Default: al
   comm=scratch|graphred                   'Execution strategy'                 Default: scratch
   spark_master=local[1]|local[2]|yarn|... 'Spark master URL'                   Default: local[worker_cores]
   deploy_mode=server|client               'Spark deploy mode'                  Default: client
   log_level=info|warn|error               'Log vebosity'                       Default: info
```

If you specify `app` to this command you get parameters for specific applications, such as `cliques`:

```
Description: Script launcher for Fractal built-in application

info: FRACTAL_HOME is set to ...
info: SPARK_HOME is set to ...
info: app is set to 'cliques'
error: inputgraph is unset

Usage:
app=fsm|motifs|cliques|cliquesopt|gquerying|gqueryingnaive|kws [OPTION]... [ALGOPTION]... fractal.sh

OPTION:
   master_memory=512m|1g|2g|...            'Master memory'                      Default: 2g
   num_workers=1|2|3|...                   'Number of workers'                  Default: 1
   worker_cores=1|2|3|...                  'Number of cores per worker'         Default: 1
   worker_memory=512m|1g|2g|...            'Workers memory'                     Default: 2g
   input_format=al|el                      'al: adjacency list; el: edge list'  Default: al
   comm=scratch|graphred                   'Execution strategy'                 Default: scratch
   spark_master=local[1]|local[2]|yarn|... 'Spark master URL'                   Default: local[worker_cores]
   deploy_mode=server|client               'Spark deploy mode'                  Default: client
   log_level=info|warn|error               'Log vebosity'                       Default: info 

ALGOPTION for 'cliques':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
```

For example, the following example submits the cliques kernel with k=2 extension steps
(i.e., cliques with k+1=3 vertices) over the dataset ```citeseer-single-label.graph```:
```
steps=2 inputgraph=$FRACTAL_HOME/data/citeseer-single-label.graph app=cliques ./bin/fractal.sh
```

## Running custom applications

You can also implement your own application using Fractal API. We provide the subproject 
"fractal-apps" to make this process easier. All you need to do is to add your application class
into ```fractal-apps/src/```, re-compile the project with ```./gradlew assemble```, and run your
code with the ```bin/fractal-custom-app.sh``` script:

```
./bin/fractal-custom-app.sh
```

For example, lets create a custom application that counts motifs with 3 vertices.
We just have to add the file ```MyMotifsApp.scala``` into
```fractal-apps/src/main/scala/br/ufmg/cs/systems/fractal/apps/```:
```
// file: fractal-apps/src/main/scala/br/ufmg/cs/systems/fractal/apps/MyMotifsApp.scala
package br.ufmg.cs.systems.fractal.apps

import br.ufmg.cs.systems.fractal._
import br.ufmg.cs.systems.fractal.pattern.Pattern
import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.hadoop.io.LongWritable
import org.apache.spark.{SparkConf, SparkContext}

object MyFractalApp extends Logging {
  def main(args: Array[String]): Unit = {
    // environment setup
    val conf = new SparkConf().setAppName("MotifsApp")
    val sc = new SparkContext(conf)
    val fc = new FractalContext(sc)
    val graphPath = args(0) // input graph
    val fgraph = fc.textFile (graphPath)

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

    val motifsMap = motifs.aggregationMap[Pattern,LongWritable](AGG_MOTIFS)
    for ((motif,count) <- motifsMap) {
      logInfo(s"motif=${motif} count=${count}")
    }

    // environment cleaning
    fc.stop()
    sc.stop()
  }
}
```

Next, we re-compile the project with ```./gradlew assemble``` and run the application over
the dataset ```data/citeseer.graph```:

```
app_class=br.ufmg.cs.systems.fractal.apps.MyMotifsApp ./bin/fractal-custom-app.sh data/citeseer.graph
```

Obs. You can use the template in ```fractal-apps/src/main/scala/br/ufmg/cs/systems/fractal/apps/MyFractalApp.scala```
for a quick start.
