#!/bin/bash
s=`date -d "last-month" +%Y-%m`
I=""
O=""
declare -a list=(activity pkg device app )
for i in "${list[@]}"; do
if [[ $I = "" ]]; then
I="$6/gender/gender_feature/train/$i/${i}_$s"
O="$6/gender/gender_model/${i}"
else
I="$I,$6/gender/gender_feature/train/$i/${i}_$s"
O="$O,$6/gender/gender_model/${i}"
fi
done

spark-submit --master $1 \
             --name "$2" \
             --jars $(echo lib/*.jar | tr ' ' ',') \
             --num-executors $3 \
             --executor-memory "$4" \
             --executor-cores $5 \
             --class com.talkingdata.model.xgboost.Train gender_classification-2.0.jar $I $O 1500 100
