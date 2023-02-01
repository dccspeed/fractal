#! /usr/bin/env bash

required="master_memory num_workers worker_memory worker_cores spark_master time_limit input_graph query"
for argname in $required; do
	if [ -z ${!argname+x} ]; then
		>&2 printf "error: $argname is unset\n"
                >&2 printf "$wholeusage\n"
		exit 1
	else
		>&2 echo "info: $argname is set to '${!argname}'"
	fi
done

this_dir=$(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)

k=$(cat $query/metadata | awk '{print $1}')

master_memory=$master_memory \
  num_workers=$num_workers \
  worker_memory=$worker_memory \
  worker_cores=$worker_cores \
  spark_master=$spark_master \
  timelimit=$time_limit \
  configs="ws_internal:true ws_external:false" \
  inputgraph=$input_graph \
  labeling=v \
  app=query_specialization_po \
  query=$query \
  steps=$((k-1)) \
  $this_dir/fractal.sh