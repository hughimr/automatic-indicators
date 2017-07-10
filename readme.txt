PART-1:
每天读取hive表的源数据
首次读取保存到oracle表中
判断hive表的源数据有没有更新


	
    
首次运行init.sh,创建项目所需要的tables，插入hive数据信息。



优化项：
1.打日志
2.控制指标更新程序不要同时启动。加入监控中


测试任务：
部署了测试脚本上午6点运行，/disk1/stat/user/liwu/qa/test/run.sh ,kdb_daily_liushui指标
AI-run下午1点运行


#项目依赖

1.依赖oracle
2.依赖data_insert_kdb插入脚本
3.依赖hive的元数据mysql表权限
4.依赖定时任务crontab
5.依赖awk