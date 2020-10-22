#!/usr/bin/env bash

if [ -z $FRACTAL_HOME ]; then
	echo "FRACTAL_HOME is unset"
	exit 1
else
	echo "info: FRACTAL_HOME is set to $FRACTAL_HOME"
fi

if [ -z $JVM_PROFILER_HOME ]; then
	echo "JVM_PROFILER_HOME is unset"
	exit 1
else
	echo "info: JVM_PROFILER_HOME is set to $JVM_PROFILER_HOME"
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

OLD_JAVA_TOOL_OPTIONS=$JAVA_TOOL_OPTIONS
#export JAVA_TOOL_OPTIONS="-javaagent:$FRACTAL_HOME/lib/aspectjweaver-1.8.10 .jar"
export JAVA_TOOL_OPTIONS="-agentpath:$JVM_PROFILER_HOME/build/libasyncProfiler.so=start,file=${file},event=${event},include=${include},interval=${interval},framebuf=5000000,traces=2000000000,flat=5000"
$FRACTAL_HOME/bin/fractal.sh "$@"
export JAVA_TOOL_OPTIONS=$OLD_JAVA_TOOL_OPTIONS
