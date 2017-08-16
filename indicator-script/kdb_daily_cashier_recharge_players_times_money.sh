#!/bin/bash
 
date=$1
yesterday=`date +%F -d "-1day"`
today=`date +%F`
date=${date:-$yesterday}
num=(`Hive -e "select count(distinct cp_order_pid),count(1),sum(cp_order_rmb)/100 from gzkdb.one_cashier_order where ds='$date' and cpid='20161021CPKDBR' and order_status=2;"`)
num1=${num[0]}
num2=${num[1]}
num3=${num[2]}
 
osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='kdb_daily_cashier_recharge_players' and ds='$date' and period='D' and origin='kdb';"
osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='kdb_daily_cashier_recharge_times' and ds='$date' and period='D' and origin='kdb';"
osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='kdb_daily_cashier_recharge_money' and ds='$date' and period='D' and origin='kdb';"

osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','kdb','kdb_daily_cashier_recharge_players','$today','$num1');"
osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','kdb','kdb_daily_cashier_recharge_times','$today','$num2');"
osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','kdb','kdb_daily_cashier_recharge_money','$today','$num3');"
