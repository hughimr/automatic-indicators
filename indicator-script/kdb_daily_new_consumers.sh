#!/bin/bash
 
date=$1
yesterday=`date +%F -d "-1day"`
today=`date +%F`
date=${date:-$yesterday}

#所有订单去掉全部退款的订单
num=`Hive -e "use gzkdb;
select count(distinct pid)
from (
select * from 
kdb_order_buy_detail
where otype in (20,23)
   and ( status>=2 or status in (-5,-6) )
   and ds='$date'
   and ds=from_unixtime(int(user_create_time/1000),'yyyy-MM-dd')
) detail 
left outer join (
select data.order_id
from (
select  order_id,
        sum(money+coupon_cost) m
from kdb_order_buy_detail
where ds='$date'
    and otype in (20,22)
group by order_id 
) data
join (
select order_id,sum(money) m
from order_refund
where status =1 
    and ds>='$date'
group by order_id 
) refund on data.order_id=refund.order_id
where data.m=refund.m
) filter on detail.order_id=filter.order_id
where filter.order_id is null;"`
 
osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='kdb_daily_new_consumers' and ds='$date' and period='D' and origin='kdb';"
osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','kdb','kdb_daily_new_consumers','$today','$num');
"
