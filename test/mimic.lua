-- Configuration
local readWriteRatio = 4 -- 4:1 ratio of reads to writes
local writeExistRatio = 10 -- 10:1 ratio of writing existing keys to new keys

local totalOperations = 1000

local metadata = nil
local shardAddresses = {}
local controllerAddress = "localhost:60081"

local function fetchMetadata()
    metadata = call(controllerAddress .. "/proto.ShardRouter/GetConfig", {})
    -- Extract shard information
    for shardId, members in pairs(metadata.members) do
        shardAddresses[shardId] = {}
        for _, member in ipairs(members.members) do
            table.insert(shardAddresses[shardId], member.addr)
        end
    end
end

-- Get shard ID for a key based on key ranges
local function getShardForKey(key)
    for shardId, ranges in pairs(metadata.shard_map) do
        for _, range in ipairs(ranges.key_ranges) do
            if key >= range.start and key < range.end then
                return shardId
            end
        end
    end
    return nil -- Key not mapped to any shard
end

fetchMetadata()

-- Function to generate random letter string
local function generateLetterKey()
    local letters = "abcdefghijklmnopqrstuvwxyz"
    local result = ""
    local length = math.floor(math.exp(math.random() * math.log(1000)))
    for i = 1, length do
        local randomIndex = math.random(#letters)
        result = result .. string.sub(letters, randomIndex, randomIndex)
    end
    return result
end

-- init client by writing some keys
local keys = {}
for i = 1, 100 do
    key = generateLetterKey()
    value = "value-" .. math.random(1, 10000)
    shardId = getShardForKey(key)
    address = shardAddresses[shardId][math.random(#shardAddresses[shardId])]
    writeRequest = { 
        clusterID = shardId,
        key = key,
        value = value
    }
    call(address .. "/proto.NodeRouter/Write", writeRequest)
    table.insert(keys, generateLetterKey())
end

-- Function to simulate a write
local function performWrite()
    local key = ""
    if math.random(writeExistRatio + 1) <= writeExistRatio then
        key = keys[math.random(#keys)]
    else
        key = generateLetterKey()
    end
    local value = "value-" .. math.random(1, 10000)
    local shardId = getShardForKey(key)
    local address = shardAddresses[shardId][math.random(#shardAddresses[shardId])]
    local writeRequest = { 
        clusterID = shardId,
        key = key, 
        value = value 
    }
    call(address .. "/proto.NodeRouter/Write", writeRequest)
    table.insert(keys, key)
end

-- Function to simulate a read
local function performRead()
    local key = keys[math.random(#keys)]
    local shardId = getShardForKey(key)
    local addresses = shardAddresses[shardId][math.random(#shardAddresses[shardId])]
    local readRequest = {
        clusterID = shardId,
        key = key
    }
    call(address .. "/proto.NodeRouter/Read", readRequest)
end

-- Perform operations
for i = 1, totalOperations do
    if math.random(readWriteRatio + 1) <= readWriteRatio then
        performRead()
    else
        performWrite()
    end
end