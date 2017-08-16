#!/bin/bash
 
date=$1
yesterday=`date +%F -d "-1day"`
today=`date +%F`
date=${date:-$yesterday}
 
num=`Hive -e "select sum(
        case when 
            order_status=2 then cp_order_rmb/100
        when order_status=5 then cp_order_rmb/100-nvl(refund_rmb/100,0)-nvl(refund_coin,0)
        else 0.0 
        end )
from gzkdb.one_cashier_order where ds='$date' and cpid='20161021CPKDBC' and order_status in (2,5);"`
 
osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='kdb_daily_cashier_real_money' and ds='$date' and period='D' and origin='kdb';"
osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','kdb','kdb_daily_cashier_real_money','$today','$num');
"
