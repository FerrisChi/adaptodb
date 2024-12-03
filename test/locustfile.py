from locust import HttpUser, task, between
import random
import json
import math
import requests

class Metadata:
    def __init__(self, client):
        self.client = client
        self.meta = {}
        self.meta_address = "http://localhost:60080"

        self.get_shard_config()

    def get_shard_config(self):
        """Fetch shard configuration from metadata controller"""
        response = self.client.get(f"{self.meta_address}/config")
        if response.status_code == 200:
            resp = response.json()
            for shard_id, krs in resp["ShardMap"].items():
                self.meta[shard_id] = {
                    "krs": krs,
                    "address": [node["HttpAddress"] for node in resp["Members"][shard_id]]
                }
            # print(self.meta)
        else:
            raise Exception(f"Failed to fetch shard config: {response.status_code}")

    def get_shard_for_key(self, key):
        if not self.meta:
            self.get_shard_config()
        for shard_id, shard_data in self.meta.items():
            for kr in shard_data["krs"]:
                if kr["start"] <= key < kr["end"]:
                    return shard_id
        return None
    
KeyCharset = "abcdefghijklmnopqrstuvwxyz"
def generate_key():
    length = math.floor(math.exp(random.random() * math.log(50)))
    result = ''.join(random.choices(KeyCharset, k=length))
    return result

ValueCharset = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
def generate_value():
    length = math.floor(math.exp(random.random() * math.log(1000)))
    result = ''.join(random.choices(ValueCharset, k=length))
    return result

class KeyValueStoreUser(HttpUser):
    wait_time = between(0.001, 1)

    def on_start(self):
        """Initialize the metadata client when the user starts"""
        self.meta = Metadata(self.client)
        self.meta.get_shard_config()  # Initial fetch of shard config
        
        # Generate some test keys
        self.test_keys = [generate_key() for _ in range(100)]
        for key in self.test_keys:
            shard_id = self.meta.get_shard_for_key(key)
            address = random.choice(self.meta.meta[shard_id]["address"])
            resp = requests.put(f"http://{address}/write", params={"shardId": str(shard_id), "key": key, "value": generate_value()})
            if resp.status_code != 200:
                # error writing key, stop the test
                raise Exception(f"Failed to write key at init: {resp.status_code, resp.text}")

    @task(30)
    def read(self):
        """Read operation with shard awareness"""
        key = random.choice(self.test_keys)
        shard_id = self.meta.get_shard_for_key(key)
        address = random.choice(self.meta.meta[shard_id]["address"])
        
        with self.client.get(
            f"http://{address}/read",
            catch_response=True,
            params={
                "shardId": str(shard_id),
                "key": key
            }
        ) as response:
            if response.status_code == 404:
                # Key not found is an acceptable response
                response.success()

    @task(10)
    def write(self):
        """Write operation with shard awareness"""
        if random.random() < 0.2:
            key = generate_key()
        else:
            key = random.choice(self.test_keys)
        value = generate_value()
        shard_id = self.meta.get_shard_for_key(key)
        address = random.choice(self.meta.meta[shard_id]["address"])
        
        self.client.put(
            f"http://{address}/write",
            params={
                "shardId": str(shard_id),
                "key": key,
                "value": value
            }
        )

    @task(1)
    def refresh_metadata(self):
        """Periodically refresh shard configuration"""
        try:
            self.meta.get_shard_config()
        except Exception as e:
            self.environment.runner.logger.error(f"Failed to refresh metadata: {e}")

# class BatchOperationsUser(KeyValueStoreUser):
#     """User class for testing batch operations"""
    
#     @task(1)
#     def batch_read(self):
#         """Batch read operation"""
#         # Select multiple random keys
#         keys = random.sample(self.test_keys, 5)
#         # Group keys by shard
#         keys_by_shard = {}
#         for key in keys:
#             shard_id = self.meta.get_shard_for_key(key)
#             if shard_id not in keys_by_shard:
#                 keys_by_shard[shard_id] = []
#             keys_by_shard[shard_id].append(key)
        
#         # Make batch requests per shard
#         for shard_id, shard_keys in keys_by_shard.items():
#             self.client.post(
#                 "/kv/batch/read",
#                 json={"keys": shard_keys},
#                 headers={"X-Shard-ID": str(shard_id)}
#             )

#     @task(1)
#     def batch_write(self):
#         """Batch write operation"""
#         # Prepare multiple key-value pairs
#         pairs = {
#             f"batch_key_{i}": f"batch_value_{random.randint(1, 1000)}"
#             for i in range(5)
#         }
        
#         # Group by shard
#         pairs_by_shard = {}
#         for key, value in pairs.items():
#             shard_id = self.meta.get_shard_for_key(key)
#             if shard_id not in pairs_by_shard:
#                 pairs_by_shard[shard_id] = {}
#             pairs_by_shard[shard_id][key] = value
        
#         # Make batch requests per shard
#         for shard_id, shard_pairs in pairs_by_shard.items():
#             self.client.post(
#                 "/kv/batch/write",
#                 json={"pairs": shard_pairs},
#                 headers={"X-Shard-ID": str(shard_id)}
#             )