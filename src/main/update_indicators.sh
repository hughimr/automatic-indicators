#!/bin/bash
#���ݵĸ���Ƶ��Ϊ��ÿ���������һ�Σ�����ͨ��LW_PJ_AI_KPI_DATA�е�insert_date�Ƿ�Ϊ�������ж������Ƿ���¹���
#cd /disk1/stat/user/liwu/qa/test
cd /disk1/stat/user/liwu/qa/AI/automatic-indicators/temp
export AI_HOME="/disk1/stat/user/liwu/qa/AI/automatic-indicators"
export AI_TEMP="$AI_HOME/temp"
#��ѯ���Ĺ��ı������,��oracle�пյ��ֶ���nn���棬��Ȼ���ļ��л�ȱʧ�ֶ�
osql_kdb -e "select t1.db,t1.tablename,nvl(t1.partition_key,'nn'),nvl(t1.partition_value,'nn'),t2.indicator,t2.run_script from LW_PJ_AI_TABLES_METADATA t1,LW_PJ_AI_INDICTOR_VS_TABLE t2 where t1.isupdated=1 and t1.db=t2.db and t1.tablename=t2.tablename;">$AI_TEMP/update_info_raw
#��LW_PJ_AI_TABLES_METADATA��isupdated��Ϊ0
osql_kdb -e "update LW_PJ_AI_TABLES_METADATA set isupdated=0 where isupdated=1;"


#������Ҫ���µ�����
awk  -F '\t' --posix '{match($4,/([[:alnum:]]{4}-[[:alnum:]]{2}-[[:alnum:]]{2})/);print substr($4, RSTART, RLENGTH),$5,$6}' OFS='\t' $AI_TEMP/update_info_raw|sort -u>$AI_TEMP/script_to_run

#��ȡ�ļ��еĴ������񣬲�ȫû�з�������������
today=`date +%F`
rm $AI_TEMP/tasks_to_run
IFS=''
while read -r line ;do 
    _date=`echo "$line"|awk -F '\t' '{print $1}'`
    _indicator=`echo "$line"|awk -F '\t' '{print $2}'`
    _run_script=`echo "$line"|awk -F '\t' '{print $3}'`
    
    if [ -n $_date ];then 
    #�õ�ָ��Ĵ��������ڵ�������������
    _data_newest=`osql_kdb -e "select insert_date from LW_PJ_AI_KPI_DATA where ds='$_date' and kpi_code='$_indicator';"`
    if [ -n "$_data_newest" -a "$_data_newest" != "$today" ];then 
    
        #_date�ǿ�,�������������ܽű����������ݵĹ����ڽű���ɣ����԰ѽű�������������
        echo "$_run_script $_date">>$AI_TEMP/tasks_to_run 
    fi
    else
    #_dateΪ��,�����ݿ��в�ѯ�Ѿ����ڵ��������ڣ��޳����Ѿ����¹�������
    _dates_to_run=(`osql_kdb -e "select ds from LW_PJ_AI_KPI_DATA where insert_date<>'$today' and kpi_code='$_indicator';"`)
    for ed in ${_dates_to_run[*]};do
        echo "$_run_script $ed">>$AI_TEMP/tasks_to_run
        done 
    fi 
    
done<$AI_TEMP/script_to_run

#ȥ�������б�����������ű�
sort -u $AI_TEMP/tasks_to_run -o $AI_TEMP/tasks_to_run

#�ָ���������
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




