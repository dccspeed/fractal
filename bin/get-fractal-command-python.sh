#!/usr/bin/env bash

version="SPARK-3.5.0"

>&2 printf "Description: Script launcher for Fractal built-in applications\n\n"

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

if [ -z $pythonscript ]; then
	>&2 echo "pythonscript is unset"
	exit 1
else
	>&2 echo "info: pythonscript is set to $pythonscript"
fi

master_memory=${master_memory:-2g}
num_workers=${num_workers:-1}
worker_cores=${worker_cores:-1}
spark_master=${spark_master:-local[${worker_cores}]}
worker_memory=${worker_memory:-2g}
labeling=${labeling:-n}
total_cores=$((num_workers * worker_cores))
deploy_mode=${deploy_mode:-client}
log_level=${log_level:-info}
timelimit=${timelimit:--1}
steptimelimit=${steptimelimit:--1}
jars=${jars:-""}
uienabled=${uienabled:-false}
app_class=${app_class:-br.ufmg.cs.systems.fractal.FractalSparkRunner}
packages="com.koloboke:koloboke-impl-jdk8:1.0.0,com.typesafe.akka:akka-remote_2.13:2.5.23,black.ninia:jep:4.2.0"
extrajavaoptions="\"-Dlog4j.configuration=file://$FRACTAL_HOME/conf/log4j.properties ${PROFILER_OPTIONS}\""

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
   $pythonscript $@"

# output command
echo "$cmd"
