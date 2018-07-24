#!/bin/bash

s=`date -d "last-month" +%Y-%m`
I=""
P=""
M=""
declare -a list=(activity app pkg device)
for i in "${list[@]}"; do
if [[ $I = "" ]]; then
I="$7/gender/gender_feature/$i/${i}_$s"
P="$8/gender_prediction/$i/${i}_$s"
M="$7/gender/gender_model/${i}"
else
I="$I,$7/gender/gender_feature/$i/${i}_$s"
P="$P,$8/gender/gender_prediction/$i/${i}_$s"
M="$M,$7/gender/gender_model/${i}"
fi
done

spark-submit --master $1 \
             --name "$2" \
             --jars $(echo lib/*.jar | tr ' ' ',') \
             --num-executors $3 \
             --executor-memory "$4" \
             --executor-cores $5 \
             --driver-memory "$6" \
             --conf spark.yarn.driver.memoryOverhead=6048 \
             --class com.talkingdata.model.xgboost.Prediction gender_classification-2.0.jar  $I $P $M
