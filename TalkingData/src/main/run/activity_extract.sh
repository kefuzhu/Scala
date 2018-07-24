#!/bin/bash
s=`date -d "last-month" +%Y-%m`
spark-submit --master $1 \
             --name "$2" \
             --jars $(echo lib/*.jar | tr ' ' ',') \
             --num-executors $3 \
             --executor-memory "$4" \
             --executor-cores $5 \
             --driver-memory "$6" \
             --class com.talkingdata.dmp.etl.extractlarge.ActivityMergeHash gender_classification-2.0.jar $7/gender/gender_feature/Activity $7/gender/gender_feature/activity/activity_$s $7/gender/feature_importance/pkg_Entropy 1 $s 30

