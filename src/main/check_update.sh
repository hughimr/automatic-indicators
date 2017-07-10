#!/bin/bash

##建立AI临时文件目录
cd /disk1/stat/user/liwu/qa/AI/automatic-indicators/temp
export AI_HOME="/disk1/stat/user/liwu/qa/AI/automatic-indicators"
export AI_TEMP="$AI_HOME/temp"

#读取hive的元数据信息,从LW_PJ_AI_INDICTOR_VS_TABLE读取database和tablename，不是读全部元数据
tables=`osql_kdb -e "select distinct tablename from LW_PJ_AI_INDICTOR_VS_TABLE ;"|awk -v RS='' -v OFS="','" 'NF { $1 = $1; print "'\''" $0 "'\''" }'`
dbs=`osql_kdb -e "select distinct db from LW_PJ_AI_INDICTOR_VS_TABLE ;"|awk -v RS='' -v OFS="','" 'NF { $1 = $1; print "'\''" $0 "'\''" }'`

MYSQL -s -e "select DBS_F.NAME,
        TBLS_f.TBL_NAME,
        IFNULL(PKEY_TBL.PKEYS,''),
        IFNULL(PKEY_INFO.PVALUES,''),
        IFNULL(PKEY_INFO.LOCATION,SDS.LOCATION)
from (SELECT * FROM DBS WHERE NAME in ($dbs)) DBS_F
left JOIN (SELECT * FROM TBLS WHERE TBL_TYPE in ('MANAGED_TABLE','EXTERNAL_TABLE') and TBL_NAME in ($tables)) TBLS_f ON DBS_F.DB_ID=TBLS_f.DB_ID
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
left JOIN SDS ON TBLS_f.SD_ID=SDS.SD_ID;">$AI_TEMP/temp_hive_raw
#去掉hive元数据路径中的hdfs://hadoop-ha-mail/
awk -F '\t' '{gsub(/hdfs:\/\/[^\/]*/,"",$5);print $0}' OFS='\t' $AI_TEMP/temp_hive_raw>$AI_TEMP/temp_hive_metadata


#解析temp_hive_metadata中集群路径到二级目录
hdfs_dir=(`awk -F '\t' '{print $5}' $AI_TEMP/temp_hive_metadata|sed -r 's#/([^/]*/[^/]*).*#/\1#g' |sort -u`)
#读取hdfs中目录对应的元数据
rm $AI_TEMP/temp_hdfs_metadata
for ed in ${hdfs_dir[*]};do 
    hadoop fs -ls -R $ed|grep ^- >>$AI_TEMP/temp_hdfs_metadata
done 

#解析hdfs数据为：目录   文件路径    文件时间
awk '{a=$8;gsub(/\/[^\/]*$/,"",$8);print $8,a,$6"_"$7}' OFS='\t' $AI_TEMP/temp_hdfs_metadata>$AI_TEMP/temp_hdfs_file

#关联路径和路径下的文件,并重新提取hdfs中没有匹配到的目录
rm $AI_TEMP/temp_hive_remain
awk -F '\t'  --re-interval 'NR==FNR{a[$5]=$0}NR>FNR{if($1 in a){print a[$1],$2,$3>"'$AI_TEMP'/temp_hive_data";b[$1]=1;}}END{for(i in a){if(b[i]!=1 && i~/^\/[^\/]*$/){print a[i]>"'$AI_TEMP'/temp_hive_remain"}}}' OFS='\t' $AI_TEMP/temp_hive_metadata $AI_TEMP/temp_hdfs_file

if [ -f "$AI_TEMP/temp_hive_remain" ];then 
#重新提取hdfs中没有匹配到的目录
rm $AI_TEMP/temp_hdfs_meta_again
IFS=''
while read line;do
    _dir=`echo $line|awk -F '\t' '{print $5}'`
    hadoop fs -ls -R $_dir|grep ^->>$AI_TEMP/temp_hdfs_meta_again
done<$AI_TEMP/temp_hive_remain
    
#--
#--解析hdfs数据为：目录   文件路径    文件时间
awk '{a=$8;gsub(/\/[^\/]*$/,"",$8);print $8,a,$6"_"$7}' OFS='\t' $AI_TEMP/temp_hdfs_meta_again>$AI_TEMP/temp_hdfs_file_again

#关联路径和路径下的文件,并重新提取hdfs中没有匹配到的目录
awk -F '\t' 'NR==FNR{a[$5]=$0}NR>FNR{if($1 in a){print a[$1],$2,$3;a[$1]=1;}}END{for(i in a){if(a[i]!=1){print a[i],"",""}}}' OFS='\t' $AI_TEMP/temp_hive_remain $AI_TEMP/temp_hdfs_file_again>>$AI_TEMP/temp_hive_data
fi

#跟数据库的数据对比，看看数据是否更新
osql_kdb -e "select partition_dir,filepath,filedate from LW_PJ_AI_TABLES_METADATA;">$AI_TEMP/temp_orl_data
awk -F '\t' 'NR==FNR{a[$1"\t"$2]=$3}NR>FNR{if(a[$5"\t"$6]==$7){}else{print $0,"1"}}' OFS='\t' $AI_TEMP/temp_orl_data $AI_TEMP/temp_hive_data>$AI_TEMP/temp_hive_final

#插入数据到oracle中,如果文件存在并且文件大小大于0
if [ -s "$AI_TEMP/temp_hive_final" ];then 
data_insert_kdb -file $AI_TEMP/temp_hive_final -table LW_PJ_AI_TABLES_METADATA -keys partition_dir,filepath
fi