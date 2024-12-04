#!/bin/bash

for i in {1..15}; do
  key="ayoo$i"
  value="ayoo$i"
  grpcurl -plaintext -d "{\"clusterID\": 1, \"key\": \"$key\", \"value\": \"$value\"}" localhost:51001 proto.NodeRouter/Write
done
