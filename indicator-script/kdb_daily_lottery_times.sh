#!/bin/bash
 
date=$1
yesterday=`date +%F -d "-1day"`
today=`date +%F`
date=${date:-$yesterday}
 
num=`Hive -e "select count(1) from gzkdb.kdb_order_buy_detail where otype=21 and ds='$date';"`
 
osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='kdb_daily_lottery_times' and ds='$date' and period='D' and origin='kdb';"
osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','kdb','kdb_daily_lottery_times','$today','$num');
"
