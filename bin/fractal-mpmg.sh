#!/usr/bin/env bash

# IMPORTANT: this version must match the "build.gradle" one
fractal_version="SPARK-2.3.2"

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

packages="com.koloboke:koloboke-impl-jdk8:1.0.0,com.typesafe.akka:akka-remote_2.11:2.5.3"
cmd="$SPARK_HOME/bin/spark-submit \\
   --class br.ufmg.cs.systems.fractal.mpmg.MPMGSparkRunner \\
   --jars $FRACTAL_HOME/fractal-core/build/libs/fractal-core-${fractal_version}.jar \\
   --packages=$packages \\
   $FRACTAL_HOME/fractal-apps/build/libs/fractal-apps-${fractal_version}.jar \\
   $config"

printf "info: Submitting command:\n$cmd\n\n"
bash -c "$cmd"
