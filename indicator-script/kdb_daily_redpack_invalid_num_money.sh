#!/bin/bash
 
date1=$1
date2=$2
yesterday=`date +%F -d "-1day"`
today=`date +%F`
sdate=${date1:-${yesterday}}
edate=${date2:-${sdate}}


#num=(`Hive -e "use gzkdb;
#select count(1),
#       nvl(sum(value-value_used),0)
#from one_player_money_bonus
#where bonus_dict_id<>660752221
#    and dead_time>=$date_f
#    and dead_time<$date_e
#    and value>value_used;"`)
#num1=${num[0]}
#num2=${num[1]}

 #使用新的统计方法
tempfile="temp$RANDOM"
Hive -e "use gzkdb;
select  from_unixtime(int(dead_time/1000),'yyyy-MM-dd'),
        count(1),
        nvl(sum(value-value_used),0)
from one_player_money_bonus
where bonus_dict_id<>660752221
    and value>value_used
    and from_unixtime(int(dead_time/1000),'yyyy-MM-dd') between '${sdate}' and '${edate}'
group by from_unixtime(int(dead_time/1000),'yyyy-MM-dd')
;"|awk -F '\t' -v to=${today} '{print "D",$1,"kdb","kdb_daily_redpack_invalid_num",to,$2;print "D",$1,"kdb","kdb_daily_redpack_invalid_money",to,$3;}' OFS='\t'>${tempfile}

data_insert_kdb -file ${tempfile} -table LW_PJ_AI_KPI_DATA -keys period,ds,origin,kpi_code && rm ${tempfile}

 
#osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='kdb_daily_redpack_invalid_num' and ds='$date' and period='D' and origin='kdb';"
#osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='kdb_daily_redpack_invalid_money' and ds='$date' and period='D' and origin='kdb';"
#
#
#osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','kdb','kdb_daily_redpack_invalid_num','$today','$num1');"
#osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','kdb','kdb_daily_redpack_invalid_money','$today','$num2');"
