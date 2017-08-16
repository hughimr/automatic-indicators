#!/bin/bash
 
date=$1
yesterday=`date +%F -d "-1day"`
today=`date +%F`
date=${date:-$yesterday}
 
num=(`Hive -e "use gzstat;
select count(distinct order_data_id),count(distinct pid),
       nvl(sum(nvl(price*amount/100,0)+nvl(carriage_cost/100,0)),0) as liushui,
       nvl(sum(nvl(money/100,0)+nvl(carriage_cost/100,0)),0) as shishou
from ai_order_buy_detail
where ds = '$date'
    and status >=2;"`)
num1=${num[0]}
num2=${num[1]}
num3=${num[2]}
num4=${num[3]}
 
osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='ai_daily_orders' and ds='$date' and period='D' and origin='ai';"
osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='ai_daily_consumers' and ds='$date' and period='D' and origin='ai';"
osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='ai_daily_liushui' and ds='$date' and period='D' and origin='ai';"
osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='ai_daily_real_money' and ds='$date' and period='D' and origin='ai';"


osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','ai','ai_daily_orders','$today','$num1');"
osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','ai','ai_daily_consumers','$today','$num2');"
osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','ai','ai_daily_liushui','$today','$num3');"
osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','ai','ai_daily_real_money','$today','$num4');"
