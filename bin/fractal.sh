#!/usr/bin/env bash

printf "Description: Script launcher for Fractal built-in applications\n\n"

gqueryings="gqueryingmcvc|gquerying|gqueryinginduced|gqueryingsampling|gqueryinginducedsampling|gqueryingnaive"
motifss="motifssampling|motifspf|motifspfmcvc|motifspflabeled|motifs"
cliquess="cliques|cliquesopt|maximalcliques"
fsms="fsm|fsmpf|fsmpflabeled|fsmpfmcvc"
enumerations="esubgraphs|vsubgraphswithedges|vsubgraphs|vsubgraphssampling"
extras="kws"
apps="${gqueryings}|${motifss}|${cliquess}|${fsms}|${enumerations}|${extras}"

usage="
Usage:
app=$apps [OPTION]... [ALGOPTION]... $(basename "$0")

OPTION:
   master_memory=512m|1g|2g|...            'Master memory'                                         Default: 2g
   num_workers=1|2|3|...                   'Number of workers'                                     Default: 1
   worker_cores=1|2|3|...                  'Number of cores per worker'                            Default: 1
   worker_memory=512m|1g|2g|...            'Workers memory'                                        Default: 2g
   input_format=al|el|sc                   'al: adjacency list; el: edge list; sc: fast reading'   Default: al
   comm=scratch|graphred                   'Execution strategy'                                    Default: scratch
   spark_master=local[1]|local[2]|yarn|... 'Spark master URL'                                      Default: local[worker_cores]
   deploy_mode=server|client               'Spark deploy mode'                                     Default: client
   log_level=info|warn|error               'Log vebosity'                                          Default: info"


if [ -z $FRACTAL_HOME ]; then
	echo "FRACTAL_HOME is unset"
	exit 1
else
	echo "info: FRACTAL_HOME is set to $FRACTAL_HOME"
fi

if [ -z $SPARK_HOME ]; then
	echo "SPARK_HOME is unset"
	exit 1
else
	echo "info: SPARK_HOME is set to $SPARK_HOME"
fi

argname="app"
if [ -z ${!argname+x} ]; then
        printf "error: $argname is unset\n"
        printf "$usage\n"
	exit 1
else
	echo "info: $argname is set to '${!argname}'"
fi

case "$app" in
	fsm)
	required="inputgraph steps fsmsupp"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   fsmsupp=<threshold>                     'Frequent Subgraph Mining absolute threshold'"
	;;
	fsmpf)
	required="inputgraph steps fsmsupp"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   fsmsupp=<threshold>                     'Frequent Subgraph Mining absolute threshold'"
	;;
	fsmpflabeled)
	required="inputgraph steps fsmsupp"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   fsmsupp=<threshold>                     'Frequent Subgraph Mining absolute threshold'"
	;;
	fsmpfmcvc)
	required="inputgraph steps fsmsupp"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   fsmsupp=<threshold>                     'Frequent Subgraph Mining absolute threshold'"
	;;
	esubgraphs)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;
	vsubgraphswithedges)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;
	vsubgraphs)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;
	vsubgraphssampling)
	required="inputgraph steps fraction"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   fraction=<fraction between 0 and 1>     'Fraction of subgraphs to be sampled uniformly at random'"
	;;
	motifssampling)
	required="inputgraph steps fraction"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   fraction=<fraction between 0 and 1>     'Fraction of subgraphs to be sampled uniformly at random'"
	;;
	motifs)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;
	motifspf)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;
	motifspfmcvc)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;
	motifspflabeled)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;
	cliques)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;
	cliquesopt)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;
	maximalcliques)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;
	gqueryingmcvc)
	required="inputgraph steps query"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   query=<query-file-path>                 'Query input file path as adjacency list. See 'data/q1-triangle.graph' for an example.'"
	;;
	gquerying)
	required="inputgraph steps query"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   query=<query-file-path>                 'Query input file path as adjacency list. See 'data/q1-triangle.graph' for an example.'"
	;;
	gqueryinginduced)
	required="inputgraph steps query"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   query=<query-file-path>                 'Query input file path as adjacency list. See 'data/q1-triangle.graph' for an example.'"
	;;
	gqueryingsampling)
	required="inputgraph steps query fraction"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   query=<query-file-path>                 'Query input file path as adjacency list. See 'data/q1-triangle.graph' for an example.'
   fraction=<fraction between 0 and 1>     'Fraction of subgraphs to be sampled uniformly at random'"
	;;
	gqueryinginducedsampling)
	required="inputgraph steps query fraction"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   query=<query-file-path>                 'Query input file path as adjacency list. See 'data/q1-triangle.graph' for an example.'
   fraction=<fraction between 0 and 1>     'Fraction of subgraphs to be sampled uniformly at random'"
	;;
	gqueryingnaive)
	required="inputgraph steps query"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   query=<query-file-path>                 'Query input file path as adjacency list. See 'data/q1-triangle.graph' for an example.'"
	;;
	kws)
	required="inputgraph steps query"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   query=\"keyword1 keyword2 ...\"           'Keywords for the query'"
	;;
	*)
	echo "Invalid application: ${app}"
	exit 1
	;;
esac

wholeusage="$usage $appusage"

for argname in $required; do
	if [ -z ${!argname+x} ]; then
		printf "error: $argname is unset\n"
    printf "$wholeusage\n"
		exit 1
	else
		echo "info: $argname is set to '${!argname}'"
	fi
done

master_memory=${master_memory:-2g}
num_workers=${num_workers:-1}
worker_cores=${worker_cores:-1}
spark_master=${spark_master:-local[${worker_cores}]}
worker_memory=${worker_memory:-2g}
input_format=${input_format:-al}
comm=${comm:-scratch}
total_cores=$((num_workers * worker_cores))
deploy_mode=${deploy_mode:-client}
log_level=${log_level:-info}
packages="com.koloboke:koloboke-impl-jdk8:1.0.0,com.typesafe.akka:akka-remote_2.11:2.5.3"

cmd="$SPARK_HOME/bin/spark-submit --master $spark_master \\
   --deploy-mode $deploy_mode \\
   --driver-memory $master_memory \\
   --conf spark.driver.extraJavaOptions=\"-Dlog4j.configuration=file://$FRACTAL_HOME/conf/log4j.properties\" \\
   --conf spark.executor.extraJavaOptions=\"-Dlog4j.configuration=file://$FRACTAL_HOME/conf/log4j.properties\" \\
   --num-executors $num_workers \\
   --executor-cores $worker_cores \\
   --executor-memory $worker_memory \\
   --class br.ufmg.cs.systems.fractal.FractalSparkRunner \\
   --jars $FRACTAL_HOME/fractal-core/build/libs/fractal-core-SPARK-2.2.0.jar \\
   --packages $packages \\
   $FRACTAL_HOME/fractal-apps/build/libs/fractal-apps-SPARK-2.2.0.jar \\
      $input_format $inputgraph $app $comm $total_cores $steps $log_level $fsmsupp $keywords $mindensity $query $fraction $configs"

printf "info: Submitting command:\n$cmd\n\n"
bash -c "$cmd"
