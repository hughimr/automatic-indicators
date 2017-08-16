#!/bin/bash
 
date=$1
yesterday=`date +%F -d "-1day"`
today=`date +%F`
date=${date:-$yesterday}
 
num=(`Hive -e "use gzstat;
select count(device_id),
       count(distinct device_id),
       count(pid),
       count(distinct pid)
from gzstat.one_countly
where ds = '$date' 
    and terminal like '%kuaiduobao%'
    and key like 'PageLoad_%';"`)
num1=${num[0]}
num2=${num[1]}
num3=${num[2]}
num4=${num[3]}
 
osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='kdb_daily_pv_device' and ds='$date' and period='D' and origin='kdb';"
osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='kdb_daily_uv_device' and ds='$date' and period='D' and origin='kdb';"
osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='kdb_daily_pv_pid' and ds='$date' and period='D' and origin='kdb';"
osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='kdb_daily_uv_pid' and ds='$date' and period='D' and origin='kdb';"


osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','kdb','kdb_daily_pv_device','$today','$num1');"
osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','kdb','kdb_daily_uv_device','$today','$num2');"
osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','kdb','kdb_daily_pv_pid','$today','$num3');"
osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','kdb','kdb_daily_uv_pid','$today','$num4');"
