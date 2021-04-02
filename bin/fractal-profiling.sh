#!/usr/bin/env bash

if [ -z $FRACTAL_HOME ]; then
	echo "FRACTAL_HOME is unset"
	exit 1
else
	echo "info: FRACTAL_HOME is set to $FRACTAL_HOME"
fi

required="file event"

for argname in $required; do
	if [ -z ${!argname+x} ]; then
		printf "error: $argname is unset\n"
		exit 1
	else
		echo "info: $argname is set to '${!argname}'"
	fi
done

interval=${interval:-10000000}
include=${include:-'*'}

profnativelib="$FRACTAL_HOME/fractal-core/src/main/resources/libasyncProfiler.so"

export PROFILER_OPTIONS="-agentpath:${profnativelib}=start,file=${file},event=${event},include=${include},interval=${interval},framebuf=5000000,traces=2000000000,flat=5000"
$FRACTAL_HOME/bin/fractal.sh "$@"
