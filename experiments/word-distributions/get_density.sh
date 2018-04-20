#!/usr/bin/env bash

logfile=$1

num_steps=$(cat $logfile | grep Superstep | wc -l)

printf "step depth word freq rfreq\n"

for step in `seq 0 $num_steps`; do
   for depth in `seq 0 $step`; do
      total=$(cat $logfile | \
         grep "^Aggregation\[previous_enumeration_${depth}\]\[${step}\]" | \
         tr ":" " " | \
         awk '{sum += $3} END {print sum}')

      cat $logfile | \
         grep "^Aggregation\[previous_enumeration_${depth}\]\[${step}\]" | \
         tr ":" " " | \
         awk -v total=$total -v step=$step -v depth=$depth '{print step,depth,$2,$3,$3/total}'
   done
done
