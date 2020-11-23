#!/usr/bin/env bash

version="SPARK-2.4.3"

printf "Description: Script launcher for Fractal built-in applications\n\n"

extras="custom"

subgraphlisting="subgraphs_listing_sf"
subgraphlisting="${subgraphlisting}|subgraphs_listing_pf"
subgraphlisting="${subgraphlisting}|induced_subgraphs_listing_sf"
subgraphlisting="${subgraphlisting}|induced_subgraphs_listing_pf"
subgraphlisting="${subgraphlisting}|induced_subgraphs_listing_pf_mcvc"
subgraphlisting="${subgraphlisting}|induced_subgraphs_listing_sample_sf"

motifss="motifs_pf"
motifss="${motifss}|motifs_pf_mcvc"
motifss="${motifss}|motifs_sample_sf"
motifss="${motifss}|motifs_sf"

cliquess="cliques_kclist_sf"
cliquess="${cliquess}|cliques_sf"
cliquess="${cliquess}|maximal_cliques_quick_sf"
cliquess="${cliquess}|maximal_cliques_pf"

fsms="fsm_sf"
fsms="${fsms}|fsm_pf"
fsms="${fsms}|fsm_pf_mcvc"

gqueryings="pattern_matching_pf_mcvc"
gqueryings="${gqueryings}|pattern_matching_pf"
gqueryings="${gqueryings}|pattern_matching_induced_pf"
gqueryings="${gqueryings}|pattern_matching_sample_pf"
gqueryings="${gqueryings}|pattern_matching_induced_sample_pf"

temporals="periodic_subgraphs_induced_sf"
temporals="${temporals}|periodic_subgraphs_induced_pf"
temporals="${temporals}|periodic_subgraphs_induced_pf_mcvc"

apps="${gqueryings}|${motifss}|${cliquess}|${fsms}|${subgraphlisting}|${temporals}${extras}"

usage="
APPS_AVAILABLE
$(echo $apps | tr '|' '\n' | sort | awk '{print "\t"$0}')

Usage:
app=<choose one from APPS_AVAILABLE> [OPTION]... [ALGOPTION]... $(basename "$0")

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
	custom)
	required=""
        appusage="

ALGOPTION for '$app':
   ** empty because this is a custom application **"
	;;

	fsm_sf)
	required="inputgraph steps fsmsupp"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   fsmsupp=<threshold>                     'Frequent Subgraph Mining absolute threshold'"
	;;

	fsm_pf)
	required="inputgraph steps fsmsupp"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   fsmsupp=<threshold>                     'Frequent Subgraph Mining absolute threshold'"
	;;

	fsm_pf_mcvc)
	required="inputgraph steps fsmsupp"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   fsmsupp=<threshold>                     'Frequent Subgraph Mining absolute threshold'"
	;;

	subgraphs_listing_sf)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;

	subgraphs_listing_pf)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;

	induced_subgraphs_listing_sf)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;

	induced_subgraphs_listing_pf)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;

	induced_subgraphs_listing_pf_mcvc)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;

	induced_subgraphs_listing_sample_sf)
	required="inputgraph steps fraction"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   fraction=<fraction between 0 and 1>     'Fraction of subgraphs to be sampled uniformly at random'"
	;;

	motifs_sample_sf)
	required="inputgraph steps fraction"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   fraction=<fraction between 0 and 1>     'Fraction of subgraphs to be sampled uniformly at random'"
	;;

	motifs_sf)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;

	motifs_pf)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target
  subgraph has size k, then steps=k-1'"
	;;

	motifs_pf_mcvc)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;

	cliques_sf)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;

	cliques_kclist_sf)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;

	maximal_cliques_quick_sf)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;

	maximal_cliques_pf)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;

	pattern_matching_pf_mcvc)
	required="inputgraph steps query"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   query=<query-file-path>                 'Query input file path as adjacency list. See 'data/q1-triangle.graph' for an example.'"
	;;

	pattern_matching_pf)
	required="inputgraph steps query"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   query=<query-file-path>                 'Query input file path as adjacency list. See 'data/q1-triangle.graph' for an example.'"
	;;

	pattern_matching_induced_pf)
	required="inputgraph steps query"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   query=<query-file-path>                 'Query input file path as adjacency list. See 'data/q1-triangle.graph' for an example.'"
	;;

	pattern_matching_sample_pf)
	required="inputgraph steps query fraction"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   query=<query-file-path>                 'Query input file path as adjacency list. See 'data/q1-triangle.graph' for an example.'
   fraction=<fraction between 0 and 1>     'Fraction of subgraphs to be sampled uniformly at random'"
	;;

	pattern_matching_induced_sample_pf)
	required="inputgraph steps query fraction"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   query=<query-file-path>                 'Query input file path as adjacency list. See 'data/q1-triangle.graph' for an example.'
   fraction=<fraction between 0 and 1>     'Fraction of subgraphs to be sampled uniformly at random'"
	;;

	keywordsearch_sf)
	required="inputgraph steps query"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   query=\"keyword1 keyword2 ...\"           'Keywords for the query'"
	;;

	periodic_subgraphs_induced_sf)
	required="inputgraph steps periodicthreshold"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   periodicthreshold=2|3|4|...             'Periodic threshold: indicates how many times subgraphs must occur with an arbitrary periodicity'"
	;;

	periodic_subgraphs_induced_pf)
	required="inputgraph steps periodicthreshold"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   periodicthreshold=2|3|4|...             'Periodic threshold: indicates how many times subgraphs must occur with an arbitrary periodicity'"
	;;

	periodic_subgraphs_induced_pf_mcvc)
	required="inputgraph steps periodicthreshold"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   periodicthreshold=2|3|4|...             'Periodic threshold: indicates how many times subgraphs must occur with an arbitrary periodicity'"
	;;

	*)
	echo "Invalid application: ${app}"
  printf "$usage\n"
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
input_format=${input_format:-sc}
comm=${comm:-scratch}
total_cores=$((num_workers * worker_cores))
deploy_mode=${deploy_mode:-client}
log_level=${log_level:-info}
jars=${jars:-""}
app_class=${app_class:-br.ufmg.cs.systems.fractal.FractalSparkRunner}
packages="com.koloboke:koloboke-impl-jdk8:1.0.0,com.typesafe.akka:akka-remote_2.11:2.5.3"
extrajavaoptions="\"-Dlog4j.configuration=file://$FRACTAL_HOME/conf/log4j.properties\""
args=${args:-"$input_format $inputgraph $app $comm $total_cores $steps $log_level $fsmsupp $keywords $mindensity $query $fraction $periodicthreshold $configs"}

cmd="$SPARK_HOME/bin/spark-submit --master $spark_master \\
   --deploy-mode $deploy_mode \\
   --driver-memory $master_memory \\
   --conf spark.driver.extraJavaOptions=$extrajavaoptions \\
   --conf spark.executor.extraJavaOptions=$extrajavaoptions \\
   --conf spark.ui.enabled=false \\
   --num-executors $num_workers \\
   --executor-cores $worker_cores \\
   --executor-memory $worker_memory \\
   --class $app_class \\
   --jars $FRACTAL_HOME/fractal-core/build/libs/fractal-core-$version.jar,$jars \\
   --packages $packages \\
   $FRACTAL_HOME/fractal-apps/build/libs/fractal-apps-$version.jar \\
   $args"

printf "info: Submitting command:\n$cmd\n\n"
bash -c "$cmd"
