#!/usr/bin/env bash

# IMPORTANT: this version must match the "build.gradle" one
fractal_version="SPARK-2.4.5"

printf "Description: Script launcher for Fractal MPMG applications\n\n"

usage="
Usage:
config=<config-file-path>.json ... $(basename "$0")"

required="config"

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

wholeusage="$usage"

for argname in $required; do
	if [ -z ${!argname+x} ]; then
		printf "error: $argname is unset\n"
                printf "$wholeusage\n"
		exit 1
	else
		echo "info: $argname is set to '${!argname}'"
	fi
done

cmd="$SPARK_HOME/bin/spark-submit \\
   --class br.ufmg.cs.systems.fractal.mpmg.MPMGSparkRunner \\
   --jars $FRACTAL_HOME/fractal-core/build/libs/fractal-core-${fractal_version}.jar \\
   $FRACTAL_HOME/fractal-apps/build/libs/fractal-apps-${fractal_version}.jar \\
   $config"

printf "info: Submitting command:\n$cmd\n\n"
bash -c "$cmd"
