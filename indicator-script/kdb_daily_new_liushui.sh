#!/bin/bash
 
date=$1
yesterday=`date +%F -d "-1day"`
today=`date +%F`
date=${date:-$yesterday}
 
num=`Hive -e "use gzkdb;
select nvl(sum(detail_liushui.m/100),0)-nvl(sum(refund_liushui.m/100),0)
from (
select detail.order_id,detail.m as m
from (
select  order_id,sum(money+coupon_cost) as m
from kdb_order_buy_detail   
where otype in(20,23) 
    and (status >= 2 or status in (-3,-4,-5,-6)) 
    and ds='$date'
    and ds=from_unixtime(int(user_create_time/1000),'yyyy-MM-dd')
group by order_id
) detail 
left outer join (select data.order_id
from (
select  order_id,
        sum(money+coupon_cost) m
from gzkdb.kdb_order_buy_detail
where ds='$date'
    and otype in (20,22)
group by order_id 
) data
join (
select order_id,sum(money) m
from gzkdb.order_refund
where status=1
    and ds>='$date'
group by order_id 
) refund on data.order_id=refund.order_id
where data.m=refund.m) ref on detail.order_id=ref.order_id
where ref.order_id is null 
) detail_liushui
left outer join (
select  order_id,sum(money) as m
from order_refund 
where order_type = 20 
    and  status =1 
    and ds='$date'
group by order_id
) refund_liushui on detail_liushui.order_id=refund_liushui.order_id;"`
 
echo $num
osql_kdb -e "delete from LW_PJ_AI_KPI_DATA where kpi_code='kdb_daily_new_liushui' and ds='$date' and period='D' and origin='kdb';"
osql_kdb -e "insert into LW_PJ_AI_KPI_DATA values ('D','$date','kdb','kdb_daily_new_liushui','$today','$num');
"
