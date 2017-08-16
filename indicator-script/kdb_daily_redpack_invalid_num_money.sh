#!/bin/bash
 
date=$1
yesterday=`date +%F -d "-1day"`
today=`date +%F`
date=${date:-$yesterday}
date_f=`date +%s000 -d "$date"`
date_e=`date +%s000 -d "1day $date"` 
num=(`Hive -e "use gzkdb;
select count(1),
       nvl(sum(value-value_used),0) 
from one_player_money_bonus 
where bonus_dict_id<>660752221 
    and dead_time>=$date_f
    and dead_time<$date_e
    and value>value_used;"`)
num1=${num[0]}
num2=${num[1]}
 
osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='kdb_daily_redpack_invalid_num' and ds='$date' and period='D' and origin='kdb';"
osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='kdb_daily_redpack_invalid_money' and ds='$date' and period='D' and origin='kdb';"


osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','kdb','kdb_daily_redpack_invalid_num','$today','$num1');"
osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','kdb','kdb_daily_redpack_invalid_money','$today','$num2');"
