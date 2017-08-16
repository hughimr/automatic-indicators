#!/bin/bash
 
date=$1
yesterday=`date +%F -d "-1day"`
today=`date +%F`
date=${date:-$yesterday}
 
num=(`Hive -e "use gzkdb;
select count(1),
       nvl(sum(value),0) 
from one_player_money_bonus 
where bonus_dict_id<>660752221 
    and ds='$date';"`)
num1=${num[0]}
num2=${num[1]}
 
osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='kdb_daily_redpack_obtain_num' and ds='$date' and period='D' and origin='kdb';"
osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='kdb_daily_redpack_obtain_money' and ds='$date' and period='D' and origin='kdb';"


osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','kdb','kdb_daily_redpack_obtain_num','$today','$num1');"
osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','kdb','kdb_daily_redpack_obtain_money','$today','$num2');"
