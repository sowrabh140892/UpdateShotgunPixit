#!/bin/sh
echo $1
COPY=$(aws s3 cp s3://$1 /tmp/shotgun.txt)
LINE=$(sed -n 1p /tmp/shotgun.txt)
echo $LINE
VAR=( $LINE )
TYPE=${VAR[0]}
ENTITY_ID=${VAR[1]}
ENTITY_TYPE=${VAR[2]}
ATTRIBUTE_NAME=${VAR[3]}
ATTRIBUTE_VALUE=${VAR[4]}
echo $TYPE
echo $ENTITY_ID
echo $ENTITY_TYPE
echo $ATTRIBUTE_NAME
echo $ATTRIBUTE_VALUE
echo $(python sns.py $TYPE $ENTITY_ID $ENTITY_TYPE $ATTRIBUTE_NAME $ATTRIBUTE_VALUE)
