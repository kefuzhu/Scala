#!/bin/bash
s=`date -d "last-month" +%Y-%m`
spark-submit --master $1 \
             --name "$2" \
             --jars $(echo lib/*.jar | tr ' ' ',') \
             --num-executors $3 \
             --executor-memory "$4" \
             --executor-cores $5 \
             --driver-memory "$6" \
             --conf spark.yarn.executor.memoryOverhead=6048\
             --class com.talkingdata.dmp.etl.extractlarge.AppPkg gender_classification-2.0.jar $8 $7/gender/gender_feature/pkg/pkg_$s $7/gender/feature_importance/pkg_Rank pkg 768

