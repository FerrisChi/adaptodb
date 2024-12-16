#!/bin/bash

ALPHA=(a b c d e f g h i j k l m n o p q r s)

for var in "${ALPHA[@]}";
do
  key="a$var"
  value="hello$var"
  grpcurl -plaintext -d "{\"clusterID\": 1, \"key\": \"$key\", \"value\": \"$value\"}" localhost:51001 proto.NodeRouter/Write
  # echo "$key, $value"
done

# sleep 60
#
# for var in "${ALPHA[@]}";
# do
#   key="b$var"
#   value="bonjour$var"
#   grpcurl -plaintext -d "{\"clusterID\": 1, \"key\": \"$key\", \"value\": \"$value\"}" localhost:51001 proto.NodeRouter/Write
# done
#
# sleep 60
#
# do
#   key="c$var"
#   value="privet$var"
#   grpcurl -plaintext -d "{\"clusterID\": 1, \"key\": \"$key\", \"value\": \"$value\"}" localhost:51001 proto.NodeRouter/Write
# done
#
