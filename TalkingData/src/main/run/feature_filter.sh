#!/bin/bash
s=`date -d "last-month" +%Y-%m`
O=""
declare -a list=(app pkg device)
for i in "${list[@]}"; do
if [[ $O = "" ]]; then
O="$7/gender/feature_importance/$i/${i}_$s"
else
O="$O,$7/gender/feature_importance/$i/${i}_$s"
fi
done

spark-submit --master $1 \
             --name "$2" \
             --jars $(echo lib/*.jar | tr ' ' ',') \
             --num-executors $3 \
             --executor-memory "$4" \
             --executor-cores $5 \
             --driver-memory "$6" \
             --conf spark.yarn.executor.memoryOverhead=6048 \
             --class com.talkingdata.dmp.etl.update.FeatureFilter gender_classification-2.0.jar $8 $O $9 pkgTimeSet,pkgTimeSet,idBox appKey,pkg,model $7/gender/feature_importance/tmp
