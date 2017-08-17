#!/bin/bash

awk '{"date +%F -d \"-"NR"days "$2"\""|getline one;print $1,$2,NR,one}' tasks_to_run|awk 'NR==1{a=$1;min=$2;max=$2;c=$4;}{if(a==$1&&c==$4){max=$2;}else{print a,min,max;a=$1;min=$2;max=$2;c=$4;}}END{print a,min,max;}'


