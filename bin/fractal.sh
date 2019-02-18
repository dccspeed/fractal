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

argname="alg"
if [ -z ${!argname+x} ]; then
	echo "$argname is unset"
	exit 1
else
	echo "$argname is set to '${!argname}'"
fi

case "$alg" in
	fsm)
	required="inputgraph steps fsmsupp"
	;;
	motifs)
	required="inputgraph steps"
	;;
	cliques)
	required="inputgraph steps"
	;;
	gquerying)
	required="inputgraph steps query"
	;;
	*)
	echo "Invalid algorithm: ${alg}"
	exit 1
	;;
esac

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

cmd="spark-submit --master $spark_master --deploy-mode $deploy_mode \\
	--driver-memory $master_memory \\
	--num-executors $num_workers \\
	--executor-cores $worker_cores \\
	--executor-memory $worker_memory \\
        --class br.ufmg.cs.systems.fractal.FractalSparkRunner \\
	--jars $FRACTAL_HOME/build/libs/fractal-SPARK-2.2.0-all.jar \\
	$FRACTAL_HOME/build/libs/fractal-SPARK-2.2.0-all.jar \\
	$inputformat $inputgraph $alg $comm $total_cores $steps info $fsmsupp $keywords $mindensity $query $configs"

echo $cmd
bash -c "$cmd"
