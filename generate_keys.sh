#!/bin/bash

# let alpha={"a", "b", "c", "d", "e", "f", "g", "h", "i"}

ALPHA=(a b c d e f g h i j k l m n o p q r s)

for var in "${ALPHA[@]}";
do
  key="a$var"
  value="yooo$var"
  grpcurl -plaintext -d "{\"clusterID\": 1, \"key\": \"$key\", \"value\": \"$value\"}" localhost:51001 proto.NodeRouter/Write
  # echo "$key, $value"
done


# for i in {1..15}; do
#   key="ab"
#   value="ayoo$i"
#   # grpcurl -plaintext -d "{\"clusterID\": 1, \"key\": \"$key\", \"value\": \"$value\"}" localhost:51001 proto.NodeRouter/Write
#   # echo "rando"
#   echo key
# done
