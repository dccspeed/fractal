#!/usr/bin/env bash

# IMPORTANT: this version must match the "build.gradle" one
fractal_version="SPARK-2.4.5"

if [ -z $FRACTAL_HOME ]; then
	echo "FRACTAL_HOME is unset"
	exit 1
else
	echo "FRACTAL_HOME is set to $FRACTAL_HOME"
fi

if [ -z $SPARK_HOME ]; then
	echo "SPARK_HOME is unset"
	exit 1
else
	echo "SPARK_HOME is set to $SPARK_HOME"
fi

required="app_class"

for argname in $required; do
	if [ -z ${!argname+x} ]; then
		echo "$argname is unset"
		exit 1
	else
		echo "$argname is set to '${!argname}'"
	fi
done

spark_master=${spark_master:-local[1]}
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

cmd="$SPARK_HOME/bin/spark-submit --master $spark_master --deploy-mode $deploy_mode \\
	--driver-memory $master_memory \\
	--num-executors $num_workers \\
	--executor-cores $worker_cores \\
	--executor-memory $worker_memory \\
        --class $app_class \\
        --packages=$packages \\
	--jars $FRACTAL_HOME/fractal-core/build/libs/fractal-core-${fractal_version}.jar \\
	$FRACTAL_HOME/fractal-apps/build/libs/fractal-apps-${fractal_version}.jar $@"

printf "info: Submitting command:\n$cmd\n\n"
bash -c "$cmd"
