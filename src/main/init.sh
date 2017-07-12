#!/bin/bash

##����oracle���ݱ�
#���ڱ����������ļ���ӳ���ϵ���ļ�����Ϣ
osql_kdb -e "create table LW_PJ_AI_TABLES_METADATA
(
  db              NVARCHAR2(50),
  tablename       NVARCHAR2(128),
  partition_key   NVARCHAR2(128),
  partition_value NVARCHAR2(128),
  partition_dir   NVARCHAR2(255),
  filepath        NVARCHAR2(255),
  filedate        NVARCHAR2(128),
  isupdated       number(2)
);"

#���ڱ���ָ������ݱ��ӳ���ϵ
osql_kdb -e "create table LW_PJ_AI_INDICTOR_VS_TABLE
(
  indicator  NVARCHAR2(128),
  db         NVARCHAR2(50),
  tablename  NVARCHAR2(128),
  run_script NVARCHAR2(500)
);"

#�û�����ָ��ļ�����
osql_kdb -e "create table LW_PJ_AI_KPI_DATA
(
  period      VARCHAR2(8),
  ds          VARCHAR2(10),
  origin      VARCHAR2(16),
  kpi_code    VARCHAR2(128),
  insert_date NVARCHAR2(10)��
  num         NUMBER
);"


##��ȡԴ������Ϣ���浽oracle��


##����AI��ʱ�ļ�Ŀ¼
AI_TEMP=/disk1/stat/user/liwu/qa/test

#��ȡhive��Ԫ������Ϣ
MYSQL -s -e "select DBS_F.NAME,
        TBLS_f.TBL_NAME,
        IFNULL(PKEY_TBL.PKEYS,''),
        IFNULL(PKEY_INFO.PVALUES,''),
        IFNULL(PKEY_INFO.LOCATION,SDS.LOCATION)
from (SELECT * FROM DBS WHERE NAME='gzkdb') DBS_F
left JOIN (SELECT * FROM TBLS WHERE TBL_TYPE in ('MANAGED_TABLE','EXTERNAL_TABLE')) TBLS_f ON DBS_F.DB_ID=TBLS_f.DB_ID
left JOIN (
SELECT TBL_ID,
        group_concat(PKEY_NAME ORDER BY INTEGER_IDX) PKEYS
FROM PARTITION_KEYS
GROUP BY TBL_ID
) PKEY_TBL ON TBLS_f.TBL_ID=PKEY_TBL.TBL_ID
left JOIN (
select TBL_ID,PVALUES,LOCATION from PARTITIONS 
inner JOIN (
SELECT PART_ID,
        group_concat(PART_KEY_VAL ORDER BY INTEGER_IDX) PVALUES
FROM PARTITION_KEY_VALS
GROUP BY PART_ID
) PVAL_TBL ON PARTITIONS.PART_ID=PVAL_TBL.PART_ID 
INNER JOIN SDS ON PARTITIONS.SD_ID=SDS.SD_ID
) PKEY_INFO ON TBLS_f.TBL_ID=PKEY_INFO.TBL_ID
left JOIN SDS ON TBLS_f.SD_ID=SDS.SD_ID;">${AI_TEMP}/temp_hive_raw
#ȥ��hiveԪ����·���е�hdfs://hadoop-ha-mail/
awk -F '\t' '{gsub(/hdfs:\/\/[^\/]*/,"",$5);print $0}' OFS='\t' ${AI_TEMP}/temp_hive_raw>${AI_TEMP}/temp_hive_metadata


#����temp_hive_metadata�м�Ⱥ·��������Ŀ¼
hdfs_dir=(`awk -F '\t' '{print $5}' ${AI_TEMP}/temp_hive_metadata|sed -r 's#/([^/]*/[^/]*).*#/\1#g' |sort -u`)
#��ȡhdfs��Ŀ¼��Ӧ��Ԫ����
rm ${AI_TEMP}/temp_hdfs_metadata
for ed in ${hdfs_dir[*]};do 
    hadoop fs -ls -R ${ed}|grep ^- >>${AI_TEMP}/temp_hdfs_metadata
done 

#����hdfs����Ϊ��Ŀ¼   �ļ�·��    �ļ�ʱ��
awk '{a=$8;gsub(/\/[^\/]*$/,"",$8);print $8,a,$6"_"$7}' OFS='\t' ${AI_TEMP}/temp_hdfs_metadata>${AI_TEMP}/temp_hdfs_file

#����·����·���µ��ļ�,��������ȡhdfs��û��ƥ�䵽��Ŀ¼
awk -F '\t' 'NR==FNR{a[$5]=$0}NR>FNR{if($1 in a){print a[$1],$2,$3>"'${AI_TEMP}'/temp_hive_data";b[$1]=1;}}END{for(i in a){if(b[i]!=1){print a[i]>"'${AI_TEMP}'/temp_hive_remain"}}}' OFS='\t' ${AI_TEMP}/temp_hive_metadata ${AI_TEMP}/temp_hdfs_file

if [ -f "$AI_TEMP/temp_hive_remain" ];then 
#������ȡhdfs��û��ƥ�䵽��Ŀ¼
rm ${AI_TEMP}/temp_hdfs_meta_again
while read line;do
    _dir=`echo ${line}|awk -F '\t' '{print $5}'`
    hadoop fs -ls -R ${_dir}|grep ^->>${AI_TEMP}/temp_hdfs_meta_again
done<${AI_TEMP}/temp_hive_remain
    
#--
#--����hdfs����Ϊ��Ŀ¼   �ļ�·��    �ļ�ʱ��
awk '{a=$8;gsub(/\/[^\/]*$/,"",$8);print $8,a,$6"_"$7}' OFS='\t' ${AI_TEMP}/temp_hdfs_meta_again>${AI_TEMP}/temp_hdfs_file_again

#����·����·���µ��ļ�,��������ȡhdfs��û��ƥ�䵽��Ŀ¼
awk -F '\t' 'NR==FNR{a[$5]=$0}NR>FNR{if($1 in a){print a[$1],$2,$3;a[$1]=1;}}END{for(i in a){if(a[i]!=1){print a[i],"",""}}}' OFS='\t' ${AI_TEMP}/temp_hive_remain ${AI_TEMP}/temp_hdfs_file_again>>${AI_TEMP}/temp_hive_data
fi

#�����Ƿ������Ϣ
awk '{print $0,"0"}' OFS='\t' ${AI_TEMP}/temp_hive_data>${AI_TEMP}/temp_hive_final

#�������ݵ�oracle��
data_insert_kdb -file ${AI_TEMP}/temp_hive_final -table LW_PJ_AI_TABLES_METADATA -keys db,tablename,partition_dir,filepath,filedate



