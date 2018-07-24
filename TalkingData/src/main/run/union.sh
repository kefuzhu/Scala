#!/bin/bash
s=`date -d "last-month" +%Y-%m`
S=""
declare -a list=(activity pkg device app)
for i in "${list[@]}"; do
if [[ $S = "" ]]; then
S="$6/gender_prediction/$i/${i}_$s"
else
S="$S,$6/gender_prediction/$i/${i}_$s"
fi
done

spark-submit --master $1 \
             --name "$2" \
             --jars $(echo lib/*.jar | tr ' ' ',') \
             --num-executors $3 \
             --executor-memory "$4" \
             --executor-cores $5 \
             --class com.talkingdata.dmp.etl.update.ModifiedPrediction gender_classification-2.0.jar $S $6/gender_prediction/result/male_$s $6/gender_prediction/result/female_$s $6/gender_prediction/sample/sample_$s 1000

