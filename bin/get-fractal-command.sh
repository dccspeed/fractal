#!/usr/bin/env bash

version="SPARK-2.4.3"

>&2 printf "Description: Script launcher for Fractal built-in applications\n\n"

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

cliquess="cliques_custom_kclist"
cliquess="${cliquess}|cliques_po"
cliquess="${cliquess}|cliques_pa"
cliquess="${cliquess}|maximal_cliques_custom_quick"
cliquess="${cliquess}|maximal_cliques_pa"
cliquess="${cliquess}|maximal_cliques_po"

quasicliquess="quasi_cliques_po"
quasicliquess="${quasicliquess}|quasi_cliques_pa"
quasicliquess="${quasicliquess}|quasi_cliques_pa_po"

fsms="fsm_po"
fsms="${fsms}|fsm_pa"
fsms="${fsms}|fsm_pa_mcvc"
fsms="${fsms}|fsm_pa_po"

gqueryings="pattern_matching_pf_mcvc"
gqueryings="${gqueryings}|pattern_matching_pf_mcvc_old"
gqueryings="${gqueryings}|pattern_matching_sf"
gqueryings="${gqueryings}|pattern_matching_pf"
gqueryings="${gqueryings}|pattern_matching_induced_pf"
gqueryings="${gqueryings}|pattern_matching_induced_pf_mcvc"
gqueryings="${gqueryings}|pattern_matching_sample_pf"
gqueryings="${gqueryings}|pattern_matching_induced_sample_pf"

temporals="periodic_subgraphs_induced_sf"
temporals="${temporals}|periodic_subgraphs_induced_pf"
temporals="${temporals}|periodic_subgraphs_induced_pf_mcvc"

subgraphsearch="induced_subgraph_search_labels_po"
subgraphsearch="${subgraphsearch}|induced_subgraph_search_labels_pa"

keywordsearch="keyword_search_po"
keywordsearch="${keywordsearch}|minimal_keyword_search_po"

patternquerygenerator="pattern_query_generator"

apps="${gqueryings}|${motifss}|${cliquess}|${quasicliquess}|${fsms}"
apps="${apps}|${subgraphlisting}|${temporals}|${subgraphsearch}"
apps="${apps}|${keywordsearch}|${extras}|${patternquerygenerator}"

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
   labeling=n|v|e|ve                       'None, vertices, edges, vertices and edges'             Default: n
   comm=scratch|graphred                   'Execution strategy'                                    Default: scratch
   spark_master=local[1]|local[2]|yarn|... 'Spark master URL'                                      Default: local[worker_cores]
   deploy_mode=server|client               'Spark deploy mode'                                     Default: client
   log_level=info|warn|error               'Log vebosity'                                          Default: info"


if [ -z $FRACTAL_HOME ]; then
	>&2 echo "FRACTAL_HOME is unset"
	exit 1
else
	>&2 echo "info: FRACTAL_HOME is set to $FRACTAL_HOME"
fi

if [ -z $SPARK_HOME ]; then
	>&2 echo "SPARK_HOME is unset"
	exit 1
else
	>&2 echo "info: SPARK_HOME is set to $SPARK_HOME"
fi

argname="app"
if [ -z ${!argname+x} ]; then
  >&2 printf "error: $argname is unset\n"
  >&2 printf "$usage\n"
	exit 1
else
	>&2 echo "info: $argname is set to '${!argname}'"
fi

case "$app" in
	custom)
	required=""
        appusage="

ALGOPTION for '$app':
   ** empty because this is a custom application **"
	;;

	fsm_po)
	required="inputgraph steps fsmsupp"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   fsmsupp=<threshold>                     'Frequent Subgraph Mining absolute threshold'"
	;;

	fsm_pa)
	required="inputgraph steps fsmsupp"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   fsmsupp=<threshold>                     'Frequent Subgraph Mining absolute threshold'"
	;;

	fsm_pa_mcvc)
	required="inputgraph steps fsmsupp"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   fsmsupp=<threshold>                     'Frequent Subgraph Mining absolute threshold'"
	;;

	fsm_pa_po)
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

	cliques_po)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;

	cliques_pa)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;

	cliques_custom_kclist)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;

	maximal_cliques_custom_quick)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;

	maximal_cliques_pa)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;

	maximal_cliques_po)
	required="inputgraph steps"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'"
	;;

	quasi_cliques_po)
	required="inputgraph steps mindensity"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   mindensity=<between 0 and 1>            'Minimum density for quasi cliques'"
	;;

	quasi_cliques_pa)
	required="inputgraph steps mindensity"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   mindensity=<between 0 and 1>            'Minimum density for quasi cliques'"
	;;

	quasi_cliques_pa_po)
	required="inputgraph steps mindensity"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   mindensity=<between 0 and 1>            'Minimum density for quasi cliques'"
	;;

	pattern_matching_pf_mcvc)
	required="inputgraph steps query"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   query=<query-file-path>                 'Query input file path as adjacency list. See 'data/q1-triangle.graph' for an example.'"
	;;

	pattern_matching_pf_mcvc_old)
	required="inputgraph steps query"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   query=<query-file-path>                 'Query input file path as adjacency list. See 'data/q1-triangle.graph' for an example.'"
	;;

	pattern_matching_sf)
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
   query=<query-file-path>                 'Query input file path as adjacency list. See 'data/q1-triangle.graph' for an example.'"
	;;

	pattern_matching_induced_pf_mcvc)
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


	induced_subgraph_search_labels_po)
	required="inputgraph steps labelsset gfiltering"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   labelsset=l1,l2,...,ln                  'Labels set: allowed labels for subgraphs'
   gfiltering=true|false                   'Graph filtering: whether input graph should be filtered before enumeration'"
	;;

	induced_subgraph_search_labels_pa)
	required="inputgraph steps labelsset gfiltering"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   labelsset=l1,l2,...,ln                  'Labels set: allowed labels for subgraphs'
   gfiltering=true|false                   'Graph filtering: whether input graph should be filtered before enumeration'"
	;;

	keyword_search_po)
	required="inputgraph steps labelsset gfiltering"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   labelsset=l1,l2,...,ln                  'Labels set: keywords'
   gfiltering=true|false                   'Graph filtering: whether input graph should be filtered before enumeration'"
	;;

	minimal_keyword_search_po)
	required="inputgraph steps labelsset gfiltering"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   labelsset=l1,l2,...,ln                  'Labels set: keywords'
   gfiltering=true|false                   'Graph filtering: whether input graph should be filtered before enumeration'"
	;;

	pattern_query_generator)
	required="inputgraph steps fraction seed topk outputdir"
        appusage="

ALGOPTION for '$app':
   inputgraph=<file-path>                  'Input graph file path'
   steps=1|2|...                           'Extension steps. If the target subgraph has size k, then steps=k-1'
   fraction=<between 0 and 1>              'Fraction of subgraphs to sample.'
   seed=<any long value>                   'Seed used for sampling and selection among top-k patterns.'
   topk=1|2|...                            'How many patterns to draw from each category'
   outputdir=<directory path dir>          'Where to store selected patterns'"
	;;

	*)
    >&2 echo "Invalid application: ${app}"
    >&2 printf "$usage\n"
	  exit 1
	;;
esac

wholeusage="$usage $appusage"

for argname in $required; do
	if [ -z ${!argname+x} ]; then
		>&2 printf "error: $argname is unset\n"
                >&2 printf "$wholeusage\n"
		exit 1
	else
		>&2 echo "info: $argname is set to '${!argname}'"
	fi
done

master_memory=${master_memory:-2g}
num_workers=${num_workers:-1}
worker_cores=${worker_cores:-1}
spark_master=${spark_master:-local[${worker_cores}]}
worker_memory=${worker_memory:-2g}
labeling=${labeling:-n}
comm=${comm:-scratch}
total_cores=$((num_workers * worker_cores))
deploy_mode=${deploy_mode:-client}
log_level=${log_level:-info}
timelimit=${timelimit:--1}
jars=${jars:-""}
uienabled=${uienabled:-false}
app_class=${app_class:-br.ufmg.cs.systems.fractal.FractalSparkRunner}
packages="com.koloboke:koloboke-impl-jdk8:1.0.0,com.typesafe.akka:akka-remote_2.11:2.5.3"
extrajavaoptions="\"-Dlog4j.configuration=file://$FRACTAL_HOME/conf/log4j.properties ${PROFILER_OPTIONS}\""
args=${args:-"$labeling $inputgraph $app $comm $total_cores $steps $log_level $timelimit $fsmsupp $mindensity $query $fraction $periodicthreshold $labelsset $gfiltering $seed $topk $outputdir $configs"}

cmd="$SPARK_HOME/bin/spark-submit --master $spark_master \\
   --deploy-mode $deploy_mode \\
   --driver-memory $master_memory \\
   --driver-java-options "${extrajavaoptions}" \\
   --conf spark.executor.extraJavaOptions=$extrajavaoptions \\
   --conf spark.ui.enabled=$uienabled \\
   --num-executors $num_workers \\
   --executor-cores $worker_cores \\
   --executor-memory $worker_memory \\
   --class $app_class \\
   --jars $FRACTAL_HOME/fractal-core/build/libs/fractal-core-$version.jar,$jars \\
   --packages $packages \\
   $FRACTAL_HOME/fractal-apps/build/libs/fractal-apps-$version.jar \\
   $args"

# output command
echo "$cmd"
