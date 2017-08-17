#!/bin/bash
 
date1=$1
date2=$2
yesterday=`date +%F -d "-1day"`
today=`date +%F`
sdate=${date1:-${yesterday}}
edate=${date2:-${sdate}}


#num=(`Hive -e "select count(distinct cp_order_pid),count(1),sum(cp_order_rmb)/100 from gzkdb.one_cashier_order where ds='$date' and cpid='20161021CPKDBR' and order_status=2;"`)
#num1=${num[0]}
#num2=${num[1]}
#num3=${num[2]}

  #使用新的统计方法
tempfile="temp$RANDOM"
Hive -e "select ds,count(distinct cp_order_pid),
                count(1),
                sum(cp_order_rmb)/100
from gzkdb.one_cashier_order
where ds between '${sdate}' and '${edate}'
    and cpid='20161021CPKDBR'
    and order_status=2
group by ds
;"|awk -F '\t' -v to=${today} '{print "D",$1,"kdb","kdb_daily_cashier_recharge_players",to,$2;print "D",$1,"kdb","kdb_daily_cashier_recharge_times",to,$3;print "D",$1,"kdb","kdb_daily_cashier_recharge_money",to,$4;}' OFS='\t'>${tempfile}

data_insert_kdb -file ${tempfile} -table LW_PJ_AI_KPI_DATA -keys period,ds,origin,kpi_code && rm ${tempfile}

#osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='kdb_daily_cashier_recharge_players' and ds='$date' and period='D' and origin='kdb';"
#osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='kdb_daily_cashier_recharge_times' and ds='$date' and period='D' and origin='kdb';"
#osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='kdb_daily_cashier_recharge_money' and ds='$date' and period='D' and origin='kdb';"
#
#osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','kdb','kdb_daily_cashier_recharge_players','$today','$num1');"
#osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','kdb','kdb_daily_cashier_recharge_times','$today','$num2');"
#osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','kdb','kdb_daily_cashier_recharge_money','$today','$num3');"
