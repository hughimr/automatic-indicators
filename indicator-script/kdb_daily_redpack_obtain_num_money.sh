#!/bin/bash
 
date1=$1
date2=$2
yesterday=`date +%F -d "-1day"`
today=`date +%F`
sdate=${date1:-${yesterday}}
edate=${date2:-${sdate}}
 
#num=(`Hive -e "use gzkdb;
#select count(1),
#       nvl(sum(value),0)
#from one_player_money_bonus
#where bonus_dict_id<>660752221
#    and ds='$date';"`)
#num1=${num[0]}
#num2=${num[1]}

  #使用新的统计方法
tempfile="temp$RANDOM"
Hive -e "use gzkdb;
select ds,count(1),
       nvl(sum(value),0)
from one_player_money_bonus
where bonus_dict_id<>660752221
    and ds between '${sdate}' and '${edate}'
group by ds
;"|awk -F '\t' -v to=${today} '{print "D",$1,"kdb","kdb_daily_redpack_obtain_num",to,$2;print "D",$1,"kdb","kdb_daily_redpack_obtain_money",to,$3;}' OFS='\t'>${tempfile}

data_insert_kdb -file ${tempfile} -table LW_PJ_AI_KPI_DATA -keys period,ds,origin,kpi_code && rm ${tempfile}

#osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='kdb_daily_redpack_obtain_num' and ds='$date' and period='D' and origin='kdb';"
#osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='kdb_daily_redpack_obtain_money' and ds='$date' and period='D' and origin='kdb';"
#
#
#osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','kdb','kdb_daily_redpack_obtain_num','$today','$num1');"
#osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','kdb','kdb_daily_redpack_obtain_money','$today','$num2');"
