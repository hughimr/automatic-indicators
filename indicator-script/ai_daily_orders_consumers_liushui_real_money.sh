#!/bin/bash
 
date1=$1
date2=$2
yesterday=`date +%F -d "-1day"`
today=`date +%F`
sdate=${date1:-${yesterday}}
edate=${date2:-${sdate}}
 
#num=(`Hive -e "use gzstat;
#select count(distinct order_data_id),count(distinct pid),
#       nvl(sum(nvl(price*amount/100,0)+nvl(carriage_cost/100,0)),0) as liushui,
#       nvl(sum(nvl(money/100,0)+nvl(carriage_cost/100,0)),0) as shishou
#from ai_order_buy_detail
#where ds = '$date'
#    and status >=2;"`)
#num1=${num[0]}
#num2=${num[1]}
#num3=${num[2]}
#num4=${num[3]}

  #使用新的统计方法
tempfile="temp$RANDOM"
Hive -e "use gzstat;
select  ds,
        count(distinct order_data_id),
        count(distinct pid),
        nvl(sum(nvl(price*amount/100,0)+nvl(carriage_cost/100,0)),0) as liushui,
        nvl(sum(nvl(money/100,0)+nvl(carriage_cost/100,0)),0) as shishou
from ai_order_buy_detail
where ds between '${sdate}' and '${edate}'
    and status >=2
group by ds
;"|awk -F '\t' -v to=${today} '{print "D",$1,"ai","ai_daily_orders",to,$2;print "D",$1,"ai","ai_daily_consumers",to,$3;print "D",$1,"ai","ai_daily_liushui",to,$4;print "D",$1,"ai","ai_daily_real_money",to,$5;}' OFS='\t'>${tempfile}

data_insert_kdb -file ${tempfile} -table LW_PJ_AI_KPI_DATA -keys period,ds,origin,kpi_code && rm ${tempfile}

#osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='ai_daily_orders' and ds='$date' and period='D' and origin='ai';"
#osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='ai_daily_consumers' and ds='$date' and period='D' and origin='ai';"
#osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='ai_daily_liushui' and ds='$date' and period='D' and origin='ai';"
#osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='ai_daily_real_money' and ds='$date' and period='D' and origin='ai';"
#
#
#osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','ai','ai_daily_orders','$today','$num1');"
#osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','ai','ai_daily_consumers','$today','$num2');"
#osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','ai','ai_daily_liushui','$today','$num3');"
#osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','ai','ai_daily_real_money','$today','$num4');"
