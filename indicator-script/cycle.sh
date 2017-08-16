#!/bin/bash

run_script=$1
f_date=$2
e_date=$3


while [ ${e_date//-/} -ge ${f_date//-/} ]
do
echo "$e_date begin"
eval  "sh $run_script $e_date 1>/dev/null 2>&1"
echo "$e_date done"
e_date=`date +%F -d "-1day $e_date"`

done
