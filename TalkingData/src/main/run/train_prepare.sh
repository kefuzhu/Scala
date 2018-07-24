#!/bin/bash
s=`date -d "last-month" +%Y-%m`
I=""
O=""
declare -a list=(activity pkg device app)
for i in "${list[@]}"; do
if [[ $I = "" ]]; then
I="$7/gender_feature/$i/${i}_$s"
O="$7/gender/gender_feature/train/$i/${i}_$s"
else
I="$I,$7/gender/gender_feature/$i/${i}_$s"
O="$O,$7/gender/gender_feature/train/$i/${i}_$s"
fi
done

spark-submit --master $1 \
             --name "$2" \
             --jars $(echo lib/*.jar | tr ' ' ',') \
             --num-executors $3 \
             --executor-memory "$4" \
             --executor-cores $5 \
             --driver-memory "$6" \
             --class com.talkingdata.dmp.etl.extractlarge.TrainsetPrep gender_classification-2.0.jar $I $O $8
