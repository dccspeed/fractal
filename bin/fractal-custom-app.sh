#!/usr/bin/env bash

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
worker_memory=${worker_memory:-2g}
inputformat=${inputformat:-al}
comm=${comm:-scratch}
total_cores=$((num_workers * worker_cores))
deploy_mode=${deploy_mode:-client}

cmd="$SPARK_HOME/bin/spark-submit --master $spark_master --deploy-mode $deploy_mode \\
	--driver-memory $master_memory \\
	--num-executors $num_workers \\
	--executor-cores $worker_cores \\
	--executor-memory $worker_memory \\
        --class $app_class \\
	--jars $FRACTAL_HOME/fractal-core/build/libs/fractal-core-SPARK-2.2.0.jar \\
	$FRACTAL_HOME/fractal-apps/build/libs/fractal-apps-SPARK-2.2.0.jar $@"

echo $cmd
bash -c "$cmd"
