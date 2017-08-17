#!/bin/bash
 
date1=$1
date2=$2
yesterday=`date +%F -d "-1day"`
today=`date +%F`
sdate=${date1:-${yesterday}}
edate=${date2:-${sdate}}
 
#num=(`Hive -e "use gzstat;
#select count(device_id),
#       count(distinct device_id),
#       count(pid),
#       count(distinct pid)
#from gzstat.one_countly
#where ds = '$date'
#    and terminal like 'ai\_%'
#    and key like 'PageLoad_%';"`)
#num1=${num[0]}
#num2=${num[1]}
#num3=${num[2]}
#num4=${num[3]}

  #使用新的统计方法
tempfile="temp$RANDOM"
Hive -e "use gzstat;
select ds,count(device_id),
       count(distinct device_id),
       count(pid),
       count(distinct pid)
from gzstat.one_countly
where ds between '${sdate}' and '${edate}'
    and terminal like 'ai\_%'
    and key like 'PageLoad_%'
group by ds
    ;"|awk -F '\t' -v to=${today} '{print "D",$1,"ai","ai_daily_pv_device",to,$2;print "D",$1,"ai","ai_daily_uv_device",to,$3;print "D",$1,"ai","ai_daily_pv_pid",to,$4;print "D",$1,"ai","ai_daily_uv_pid",to,$5;}' OFS='\t'>${tempfile}

data_insert_kdb -file ${tempfile} -table LW_PJ_AI_KPI_DATA -keys period,ds,origin,kpi_code && rm ${tempfile}


#osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='ai_daily_pv_device' and ds='$date' and period='D' and origin='ai';"
#osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='ai_daily_uv_device' and ds='$date' and period='D' and origin='ai';"
#osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='ai_daily_pv_pid' and ds='$date' and period='D' and origin='ai';"
#osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='ai_daily_uv_pid' and ds='$date' and period='D' and origin='ai';"
#
#
#osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','ai','ai_daily_pv_device','$today','$num1');"
#osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','ai','ai_daily_uv_device','$today','$num2');"
#osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','ai','ai_daily_pv_pid','$today','$num3');"
#osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','ai','ai_daily_uv_pid','$today','$num4');"
