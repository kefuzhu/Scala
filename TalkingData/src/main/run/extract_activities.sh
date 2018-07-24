#!/bin/sh
d=`date -d "last-day" +%Y-%m-%d`
s=`date -d "last-day" +%Y/%m/%d`
spark-submit --master $1 \
             --name "$2" \
             --jars $(echo lib/*.jar | tr ' ' ',') \
             --num-executors $3 \
             --executor-memory "$4" \
             --executor-cores $5 \
             --class com.talkingdata.dmp.etl.extract.ActivityExtract gender_classification-2.0.jar /datascience/data/data-writer/ta/tdcv3/data/$s/*/* $6/gender/gender_feature/Activity/$d
