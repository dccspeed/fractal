#!/usr/bin/env bash

version="SPARK-2.4.3"

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

required="app_class args file event"

for argname in $required; do
	if [ -z ${!argname+x} ]; then
		echo "$argname is unset"
		exit 1
	else
		echo "$argname is set to '${!argname}'"
	fi
done

app="custom" app_class=$app_class $FRACTAL_HOME/bin/fractal-profiling.sh "$@"
