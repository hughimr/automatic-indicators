#!/bin/bash

#cd dir

cd /disk1/stat/user/liwu/qa/AI/automatic-indicators/temp
export AI_HOME="/disk1/stat/user/liwu/qa/AI/automatic-indicators"
export AI_TEMP="$AI_HOME/temp"
#检查更新
sh $AI_HOME/src/main/check_update.sh 

#更新指标
sh $AI_HOME/src/main/update_indicators.sh 