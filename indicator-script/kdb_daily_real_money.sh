#!/bin/bash
 
date1=$1
date2=$2
yesterday=`date +%F -d "-1day"`
today=`date +%F`
sdate=${date1:-${yesterday}}
edate=${date2:-${sdate}}
 
#num=`Hive -e "use gzkdb;
#select (nvl(detail_liushui.m,0)-nvl(refund_liushui.m,0))/100
#from (
#select  sum(money) as m
#from kdb_order_buy_detail
#where otype in(20,23)
#    and (status >= 2 or status in (-3,-4,-5,-6))
#    and ds='$date'
#) detail_liushui
#join (
#select  sum(money) as m
#from order_refund
#where order_type = 20
#    and  status =1
#    and money_type = 0
#    and ds='$date'
#) refund_liushui on true
#;"`
 #使用新的统计方法
tempfile="temp$RANDOM"
Hive -e "use gzkdb;
select  ds,sum(money)/100
from kdb_order_buy_detail
where otype in(20,23)
    and status >= 2
    and ds between '${sdate}' and '${edate}'
group by ds
;"|awk -F '\t' -v to=${today} '{print "D",$1,"kdb","kdb_daily_real_money",to,$2}' OFS='\t'>${tempfile}

data_insert_kdb -file ${tempfile} -table LW_PJ_AI_KPI_DATA -keys period,ds,origin,kpi_code && rm ${tempfile}

#osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='kdb_daily_real_money' and ds='$date' and period='D' and origin='kdb';"
#osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','kdb','kdb_daily_real_money','$today','$num');
#"
