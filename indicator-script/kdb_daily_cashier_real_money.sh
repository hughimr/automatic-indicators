#!/bin/bash
 
date1=$1
date2=$2
yesterday=`date +%F -d "-1day"`
today=`date +%F`
sdate=${date1:-${yesterday}}
edate=${date2:-${sdate}}
 
#num=`Hive -e "select sum(
#                        case when
#                            order_status=2 then cp_order_rmb/100
#                        when order_status=5 then cp_order_rmb/100-nvl(refund_rmb/100,0)-nvl(refund_coin,0)
#                        else 0.0
#                        end )
#from gzkdb.one_cashier_order
#where ds='$date'
#    and cpid='20161021CPKDBC'
#    and order_status in (2,5)
#;"`

  #使用新的统计方法
tempfile="temp$RANDOM"
Hive -e "select ds,sum(
                        case when
                            order_status=2 then cp_order_rmb/100
                        when order_status=5 then cp_order_rmb/100-nvl(refund_rmb/100,0)-nvl(refund_coin,0)
                        else 0.0
                        end )
from gzkdb.one_cashier_order
where ds between '${sdate}' and '${edate}'
    and cpid='20161021CPKDBC'
    and order_status in (2,5)
group by ds
;"|awk -F '\t' -v to=${today} '{print "D",$1,"kdb","kdb_daily_cashier_real_money",to,$2}' OFS='\t'>${tempfile}

data_insert_kdb -file ${tempfile} -table LW_PJ_AI_KPI_DATA -keys period,ds,origin,kpi_code && rm ${tempfile}


#osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='kdb_daily_cashier_real_money' and ds='$date' and period='D' and origin='kdb';"
#osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','kdb','kdb_daily_cashier_real_money','$today','$num');
#"
