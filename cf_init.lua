--[[
    Creates new Cuckoo Filter.
    Accepted parameters:
        Keys:
            key_name - name of Redis string key which backs this filter
        Arguments:
            number_of_buckets - number of buckets in filter, must be power of two
            bits_per_fingerprint - size of each fingerprint in bits
            bucket_size - number of keys (fingerprints) in each bucket
--]]

local function is_power_of_two(x)
    return bit.band(x, x-1) == 0
end

assert(redis.replicate_commands(),
    "assertion failed - script oplog replication is not supported")

local number_of_buckets = tonumber(ARGV[1])
assert(number_of_buckets and number_of_buckets > 1 and number_of_buckets <= 0x100000000,
    "number_of_buckets: positive integer > 2 and <= 2^29 expected")
assert(is_power_of_two(number_of_buckets),
    "number_of_buckets: must be power of two (2^n)")

local bucket_size = tonumber(ARGV[2])
assert(bucket_size and bucket_size >= 2 and bucket_size <= 8,
    "bucket_size: positive integer >= 2 and <= 8 expected")

local bits_per_fingerprint = tonumber(ARGV[3])
assert(bits_per_fingerprint and bits_per_fingerprint >= 7 and bits_per_fingerprint <= 128,
    "bits_per_fingerprint: positive integer >= 7 and <= 128 expected")

local total_bits = number_of_buckets * bits_per_fingerprint * bucket_size
local total_bytes = math.ceil(total_bits / 8)
assert(total_bytes <= 536870900,
    "filter with such params cannot fit redis string")

for i, key_name in ipairs(KEYS) do
    redis.call("setbit", key_name, 6 * 8 + total_bits - 1, 0)
    redis.call("bitfield", key_name,
        "set", "u32", 0, number_of_buckets, 
        "set", "u8",  0 + 32, bits_per_fingerprint,
        "set", "u8",  0 + 32 + 8, bucket_size)
end

return true
