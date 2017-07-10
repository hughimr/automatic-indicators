#!/bin/bash
#数据的更新频率为，每天下午更新一次，所以通过LW_PJ_AI_KPI_DATA中的insert_date是否为今天来判断数据是否更新过。
#cd /disk1/stat/user/liwu/qa/test
cd /disk1/stat/user/liwu/qa/AI/automatic-indicators/temp
export AI_HOME="/disk1/stat/user/liwu/qa/AI/automatic-indicators"
export AI_TEMP="$AI_HOME/temp"
#查询更改过的表和日期,把oracle中空的字段用nn代替，不然在文件中会缺失字段
osql_kdb -e "select t1.db,t1.tablename,nvl(t1.partition_key,'nn'),nvl(t1.partition_value,'nn'),t2.indicator,t2.run_script from LW_PJ_AI_TABLES_METADATA t1,LW_PJ_AI_INDICTOR_VS_TABLE t2 where t1.isupdated=1 and t1.db=t2.db and t1.tablename=t2.tablename;">$AI_TEMP/update_info_raw
#把LW_PJ_AI_TABLES_METADATA中isupdated变为0
osql_kdb -e "update LW_PJ_AI_TABLES_METADATA set isupdated=0 where isupdated=1;"


#解析需要更新的日期
awk  -F '\t' --posix '{match($4,/([[:alnum:]]{4}-[[:alnum:]]{2}-[[:alnum:]]{2})/);print substr($4, RSTART, RLENGTH),$5,$6}' OFS='\t' $AI_TEMP/update_info_raw|sort -u>$AI_TEMP/script_to_run

#提取文件中的待跑任务，补全没有分区的重跑日期
today=`date +%F`
rm $AI_TEMP/tasks_to_run
IFS=''
while read -r line ;do 
    _date=`echo "$line"|awk -F '\t' '{print $1}'`
    _indicator=`echo "$line"|awk -F '\t' '{print $2}'`
    _run_script=`echo "$line"|awk -F '\t' '{print $3}'`
    
    if [ -n $_date ];then 
    #拿到指标的待重跑日期的最新数据日期
    _data_newest=`osql_kdb -e "select insert_date from LW_PJ_AI_KPI_DATA where ds='$_date' and kpi_code='$_indicator';"`
    if [ -n "$_data_newest" -a "$_data_newest" != "$today" ];then 
    
        #_date非空,按日期运行重跑脚本，插入数据的工作在脚本完成，可以把脚本加入任务监控中
        echo "$_run_script $_date">>$AI_TEMP/tasks_to_run 
    fi
    else
    #_date为空,从数据库中查询已经存在的数据日期，剔除掉已经更新过的日期
    _dates_to_run=(`osql_kdb -e "select ds from LW_PJ_AI_KPI_DATA where insert_date<>'$today' and kpi_code='$_indicator';"`)
    for ed in ${_dates_to_run[*]};do
        echo "$_run_script $ed">>$AI_TEMP/tasks_to_run
        done 
    fi 
    
done<$AI_TEMP/script_to_run

#去重任务列表，多进程启动脚本
sort -u $AI_TEMP/tasks_to_run -o $AI_TEMP/tasks_to_run

#分割任务运行
nums=$(expr $(cat tasks_to_run |wc -l) / 15)
split -l $nums tasks_to_run -d -a 2 tasks_to_run_PART
IFS=$'\n' 
for taskFile in `ls tasks_to_run_PART*`
do 

IFS=''
while read -r task;do
    eval nohup sh "$task" &
    done<$AI_TEMP/$taskFile
sleep 300
done 

rm tasks_to_run_PART*




