#!/usr/bin/env bash

if [ -z $FRACTAL_HOME ]; then
	>&2 echo "FRACTAL_HOME is unset"
	exit 1
else
	>&2 echo "info: FRACTAL_HOME is set to $FRACTAL_HOME"
fi

cmd=$($FRACTAL_HOME/bin/get-fractal-command.sh)

if [ -z "$cmd" ]; then
	exit 1
fi

>&2 printf "info: Submitting command:\n$cmd\n\n"
bash -c "$cmd"
