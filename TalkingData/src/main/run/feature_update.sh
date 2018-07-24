#!/bin/bash
s=`date -d "last-month" +%Y-%m`
I=""
OE=""
OR=""
declare -a list=(app pkg device)
for i in "${list[@]}"; do
if [[ $I = "" ]]; then
I="$6/gender/feature_importance/$i/${i}_$s"
OE="$6/gender/feature_importance/${i}_Entropy"
OR="$6/gender/feature_importance/${i}_Rank"
else
I="$I,$6/gender/feature_importance/$i/${i}_$s"
OE="$OE,$6/gender/feature_importance/${i}_Entropy"
OR="$OR,$6/gender/feature_importance/${i}_Rank"
fi
done

spark-submit --master $1 \
             --name "$2" \
             --jars $(echo lib/*.jar | tr ' ' ',') \
             --num-executors $3 \
             --executor-memory "$4" \
             --executor-cores $5 \
             --class com.talkingdata.dmp.etl.update.FeatureUpdate_v2 gender_classification-2.0.jar $I $OE $OR
