#!/bin/bash
 
date1=$1
date2=$2
yesterday=`date +%F -d "-1day"`
today=`date +%F`
sdate=${date1:-${yesterday}}
edate=${date2:-${sdate}}


 
#num=`Hive -e "select count(1) from gzkdb.kdb_order_buy_detail where otype=21 and ds='$date';"`

 #使用新的统计方法
tempfile="temp$RANDOM"
Hive -e "use gzkdb;
select  ds,count(1)
from kdb_order_buy_detail
where otype=21
    and ds between '${sdate}' and '${edate}'
group by ds
;"|awk -F '\t' -v to=${today} '{print "D",$1,"kdb","kdb_daily_lottery_times",to,$2}' OFS='\t'>${tempfile}

data_insert_kdb -file ${tempfile} -table LW_PJ_AI_KPI_DATA -keys period,ds,origin,kpi_code && rm ${tempfile}




#osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='kdb_daily_lottery_times' and ds='$date' and period='D' and origin='kdb';"
#osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','kdb','kdb_daily_lottery_times','$today','$num');
#"
