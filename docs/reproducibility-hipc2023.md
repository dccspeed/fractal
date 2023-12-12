# Graph Pattern Mining Paradigms: Consolidation and Renewed Bearing (HiPC' 23)

In this work we study different Graph Pattern Mining (GPM) paradigms over the
same GPM system Fractal.

## Reproducibility

We assume a few environment variables are exported: ```JAVA_HOME``` points to a
OpenJDK-8 installation; ```SPARK_HOME``` points to your Spark package
installation; ```FRACTAL_HOME``` points to
Fractal installation (provided under ```code/fractal```).

1. Clone Fractal repository (branch ```hipc2023```):
```aidl
git clone -b hipc2023 git@github.com:dccspeed/fractal.git fractal
export FRACTAL_HOME="$(pwd)/fractal"
```

2. Follow instructions for building Fractal in ```$FRACTAL_HOME/README.md```.
3. Download [data from GDrive](https://drive.google.com/drive/folders/1ViLAlQt45hFDtqTCJnOfqk4WZ71E3IUN) and place the content on ```$FRACTAL_HOME/data```.
4. Follow application specific instructions below.


Common parameters are described next:
* Let ```<master_memory>``` maximum allowed resident memory on coordinator node
  / master (e.g.
  5g, 10g);
* Let ```<num_workers>``` be the number of processes (machines) used in parallel;
* Let ```<worker_memory>``` maximum allowed resident memory on each worker (e.g.
  10g, 5g, etc);
* Let ```<worker_cores>``` be the number of process thread (vcores) to be used
  on each worker;
* Let ```<spark_master>``` be the [master url](https://spark.apache.org/docs/2.4.0/submitting-applications.html#master-urls) used by Spark to indicate how the
  cluster resources are managed;
* Let ```<time_limit>``` be the time budget in milliseconds (ms) to be used in
  this experiments (e.g. five hours (in ms) = 18000000);
* Let ```<input_graph>``` be the graph directory used as input (e.g. ```data/mico```);

## Pattern querying ($\rho$-$PQ$)

* Let ```<rho>``` be a directory representing a pattern query;

#### Pattern-oblivious (POSE)

```
$ master_memory=<master_memory> \
    num_workers=<num_workers> \
    worker_memory=<worker_memory> \
    worker_cores=<worker_cores> \
    spark_master=<spark_master> \
    time_limit=<time_limit> \
    input_graph=<input_graph> \
    query=<rho> \
    $FRACTAL_HOME/bin/patternquerying-pose.sh
```


#### Pattern-aware (PASE)

```
$ master_memory=<master_memory> \
    num_workers=<num_workers> \
    worker_memory=<worker_memory> \
    worker_cores=<worker_cores> \
    spark_master=<spark_master> \
    time_limit=<time_limit> \
    input_graph=<input_graph> \
    query=<rho> \
    $FRACTAL_HOME/bin/patternquerying-pase.sh
```

#### Minimum Connected Vertex Cover (Custom)

```
$ master_memory=<master_memory> \
    num_workers=<num_workers> \
    worker_memory=<worker_memory> \
    worker_cores=<worker_cores> \
    spark_master=<spark_master> \
    time_limit=<time_limit> \
    input_graph=<input_graph> \
    query=<rho> \
    $FRACTAL_HOME/bin/patternquerying-custom.sh
```

### Frequent subgraph mining ($k$-$FSM$-$\alpha$)

* Let ```<k>``` be the number of edges in the pattern;
* Let ```<alpha>``` be the minimum image support value (MIS);

#### Pattern-oblivious (POSE)

```
$ master_memory=<master_memory> \
    num_workers=<num_workers> \
    worker_memory=<worker_memory> \
    worker_cores=<worker_cores> \
    spark_master=<spark_master> \
    time_limit=<time_limit> \
    input_graph=<input_graph> \
    k=<k> \
    minsupp=<alpha> \
    $FRACTAL_HOME/bin/fsm-pose.sh
```

#### Pattern-aware (PASE)
```
$ master_memory=<master_memory> \
    num_workers=<num_workers> \
    worker_memory=<worker_memory> \
    worker_cores=<worker_cores> \
    spark_master=<spark_master> \
    time_limit=<time_limit> \
    input_graph=<input_graph> \
    k=<k> \
    minsupp=<alpha> \
    $FRACTAL_HOME/bin/fsm-pase.sh
```


#### Hybrid (PASE+POSE)

```
$ master_memory=<master_memory> \
    num_workers=<num_workers> \
    worker_memory=<worker_memory> \
    worker_cores=<worker_cores> \
    spark_master=<spark_master> \
    time_limit=<time_limit> \
    input_graph=<input_graph> \
    k=<k> \
    minsupp=<alpha> \
    $FRACTAL_HOME/bin/fsm-pase-pose.sh
```

### Quasi cliques ($k$-$QC$-$\alpha$)

* Let ```<k>``` be the number of vertices in the quasi clique;
* Let ```<alpha>``` be minimum density for be considered a quasi clique;


#### Pattern-oblivious (POSE)
```
$ master_memory=<master_memory> \
    num_workers=<num_workers> \
    worker_memory=<worker_memory> \
    worker_cores=<worker_cores> \
    spark_master=<spark_master> \
    time_limit=<time_limit> \
    input_graph=<input_graph> \
    k=<k> \
    density=<alpha> \
    $FRACTAL_HOME/bin/quasicliques-pose.sh
```
#### Pattern-aware (PASE)
```
$ master_memory=<master_memory> \
    num_workers=<num_workers> \
    worker_memory=<worker_memory> \
    worker_cores=<worker_cores> \
    spark_master=<spark_master> \
    time_limit=<time_limit> \
    input_graph=<input_graph> \
    k=<k> \
    density=<alpha> \
    $FRACTAL_HOME/bin/quasicliques-pase.sh
```
#### Hybrid (PASE+POSE)

```
$ master_memory=<master_memory> \
    num_workers=<num_workers> \
    worker_memory=<worker_memory> \
    worker_cores=<worker_cores> \
    spark_master=<spark_master> \
    time_limit=<time_limit> \
    input_graph=<input_graph> \
    k=<k> \
    density=<alpha> \
    $FRACTAL_HOME/bin/quasicliques-pase-pose.sh
```

### Query specialization ($\rho$-$QS$)
* Let ```<rho>``` be a directory representing a pattern query;

#### Pattern-oblivious (POSE)
```
$ master_memory=<master_memory> \
    num_workers=<num_workers> \
    worker_memory=<worker_memory> \
    worker_cores=<worker_cores> \
    spark_master=<spark_master> \
    time_limit=<time_limit> \
    input_graph=<input_graph> \
    query=<rho> \
    $FRACTAL_HOME/bin/queryspecialization-pose.sh
```
#### Pattern-aware (PASE)
```
$ master_memory=<master_memory> \
    num_workers=<num_workers> \
    worker_memory=<worker_memory> \
    worker_cores=<worker_cores> \
    spark_master=<spark_master> \
    time_limit=<time_limit> \
    input_graph=<input_graph> \
    query=<rho> \
    $FRACTAL_HOME/bin/queryspecialization-pase.sh
```
#### Hybrid (PASE+POSE)
```
$ master_memory=<master_memory> \
    num_workers=<num_workers> \
    worker_memory=<worker_memory> \
    worker_cores=<worker_cores> \
    spark_master=<spark_master> \
    time_limit=<time_limit> \
    input_graph=<input_graph> \
    query=<rho> \
    $FRACTAL_HOME/bin/queryspecialization-pase-pose.sh
```

### Label search ($k$-$LS$-$\mathcal{L}$)
* Let ```<k>``` be the number of vertices in the subgraph;
* Let ```<labelset>``` be label set used as query;

#### Pattern-oblivious (POSE)
```
$ master_memory=<master_memory> \
    num_workers=<num_workers> \
    worker_memory=<worker_memory> \
    worker_cores=<worker_cores> \
    spark_master=<spark_master> \
    time_limit=<time_limit> \
    input_graph=<input_graph> \
    k=<k> \
    labelsset=<labelset> \
    $FRACTAL_HOME/bin/labelsearch-pose.sh
```

#### Pattern-aware (PASE)
```
$ master_memory=<master_memory> \
    num_workers=<num_workers> \
    worker_memory=<worker_memory> \
    worker_cores=<worker_cores> \
    spark_master=<spark_master> \
    time_limit=<time_limit> \
    input_graph=<input_graph> \
    k=<k> \
    labelsset=<labelset> \
    $FRACTAL_HOME/bin/labelsearch-pase.sh
```

#### Pattern-oblivious with graph filtering (POSE+GF)

```
$ master_memory=<master_memory> \
    num_workers=<num_workers> \
    worker_memory=<worker_memory> \
    worker_cores=<worker_cores> \
    spark_master=<spark_master> \
    time_limit=<time_limit> \
    input_graph=<input_graph> \
    k=<k> \
    labelsset=<labelset> \
    $FRACTAL_HOME/bin/labelsearch-pose-gf.sh
```

### Cliques ($k$-$CL$)

* Let ```<k>``` be the number of vertices in the mined cliques;

#### Pattern-oblivious (POSE)

```
$ master_memory=<master_memory> \
    num_workers=<num_workers> \
    worker_memory=<worker_memory> \
    worker_cores=<worker_cores> \
    spark_master=<spark_master> \
    time_limit=<time_limit> \
    input_graph=<input_graph> \
    k=<k> \
    $FRACTAL_HOME/bin/cliques-pose.sh
```


#### Pattern-aware (PASE)

```
$ master_memory=<master_memory> \
    num_workers=<num_workers> \
    worker_memory=<worker_memory> \
    worker_cores=<worker_cores> \
    spark_master=<spark_master> \
    time_limit=<time_limit> \
    input_graph=<input_graph> \
    k=<k> \
    $FRACTAL_HOME/bin/cliques-pase.sh
```

#### Kclist (Custom)

```
$ master_memory=<master_memory> \
    num_workers=<num_workers> \
    worker_memory=<worker_memory> \
    worker_cores=<worker_cores> \
    spark_master=<spark_master> \
    time_limit=<time_limit> \
    input_graph=<input_graph> \
    k=<k> \
    $FRACTAL_HOME/bin/cliques-custom.sh
```

### Minimal keyword search ($k$-$MKS$-$\mathcal{K}$)

* Let ```<k>``` be the number of vertices in the subgraph;
* Let ```<labelset>``` be label set used as keywords to search;

#### Pattern-oblivious (POSE)
```
$ master_memory=<master_memory> \
    num_workers=<num_workers> \
    worker_memory=<worker_memory> \
    worker_cores=<worker_cores> \
    spark_master=<spark_master> \
    time_limit=<time_limit> \
    input_graph=<input_graph> \
    k=<k> \
    labelsset=<labelset> \
    $FRACTAL_HOME/bin/minimalkeywordsearch-pose.sh
```

#### Pattern-aware (PASE)

```
$ master_memory=<master_memory> \
    num_workers=<num_workers> \
    worker_memory=<worker_memory> \
    worker_cores=<worker_cores> \
    spark_master=<spark_master> \
    time_limit=<time_limit> \
    input_graph=<input_graph> \
    k=<k> \
    labelsset=<labelset> \
    $FRACTAL_HOME/bin/minimalkeywordsearch-pase.sh
```

#### Pattern-oblivious with graph filtering (POSE+GF)


```
$ master_memory=<master_memory> \
    num_workers=<num_workers> \
    worker_memory=<worker_memory> \
    worker_cores=<worker_cores> \
    spark_master=<spark_master> \
    time_limit=<time_limit> \
    input_graph=<input_graph> \
    k=<k> \
    labelsset=<labelset> \
    $FRACTAL_HOME/bin/minimalkeywordsearch-pose-gf.sh
```