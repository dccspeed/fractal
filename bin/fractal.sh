#!/usr/bin/env bash

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )"
export FRACTAL_HOME="$SCRIPT_DIR/.."

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
