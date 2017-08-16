#!/bin/bash
 
date=$1
yesterday=`date +%F -d "-1day"`
today=`date +%F`
date=${date:-$yesterday}
 
num=`Hive -e "use gzkdb;
select count(1) 
from one_user_base_info
where ds='$date';"`
 
osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='kdb_daily_new_players' and ds='$date' and period='D' and origin='kdb';"
osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','kdb','kdb_daily_new_players','$today','$num');
"
