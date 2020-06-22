#!/usr/bin/env bash

# IMPORTANT: this version must match the "build.gradle" one
fractal_version="SPARK-2.4.5"

printf "Description: Script launcher for Fractal custom applications\n\n"

usage="
Usage:
[OPTION]... $(basename "$0")

OPTION:
   app_class=br.ufmg.cs.systems.fractal.apps.MyMotifsApp|...   'App main method class'              Default:  br.ufmg.cs.systems.fractal.apps.MyMotifsApp
   master_memory=512m|1g|2g|...                                'Master memory'                      Default:  2g
   num_workers=1|2|3|...                                       'Number of workers'                  Default:  1
   worker_cores=1|2|3|...                                      'Number of cores per worker'         Default:  1
   worker_memory=512m|1g|2g|...                                'Workers memory'                     Default:  2g
   spark_master=local[1]|local[2]|yarn|...                     'Spark master URL'                   Default:  local[worker_cores]
   deploy_mode=server|client                                   'Spark deploy mode'                  Default:  client
   log_level=info|warn|error                                   'Log vebosity'                       Default:  info"

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

master_memory=${master_memory:-2g}
num_workers=${num_workers:-1}
worker_cores=${worker_cores:-1}
spark_master=${spark_master:-local[${worker_cores}]}
worker_memory=${worker_memory:-2g}
deploy_mode=${deploy_mode:-client}
log_level=${log_level:-info}

cmd="$SPARK_HOME/bin/spark-submit \\
   --master $spark_master \\
   --deploy-mode $deploy_mode \\
   --driver-memory $master_memory \\
   --num-executors $num_workers \\
   --executor-cores $worker_cores \\
   --executor-memory $worker_memory \\
   --class $app_class \\
   --jars $FRACTAL_HOME/fractal-core/build/libs/fractal-core-${fractal_version}.jar \\
   $FRACTAL_HOME/fractal-apps/build/libs/fractal-apps-${fractal_version}.jar \\
   $@"

printf "info: Submitting command:\n$cmd\n\n"
bash -c "$cmd"
