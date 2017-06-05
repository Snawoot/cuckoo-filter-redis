--[[
    Check presence of element in existing Cuckoo Filter.
    Accepted parameters:
        Keys:
            key_name - name of Redis string key with Cuckoo filter
        Arguments:
            elem ... - string with element to check
        Return value:
            vector of true or false values corresponsing to presence of given element
--]]
local hdr_size = 6
local bytemask = 0xFF

local function hash32(key, seed)
    local c1 = 0xcc9e2d51
    local c2 = 0x1b873593
    local r1 = 15
    local r2 = 13
    local m = 5
    local n = 0xe6546b64
    if seed == nil then
        seed = 0
    end

    local function multiply(x, y)
        -- This is required to emulate uint32 overflow correctly -- otherwise,
        -- higher order bits are simply truncated and discarded.
        return (bit.band(x, 0xffff) * y) + bit.lshift(bit.band(bit.rshift(x, 16) * y,  0xffff), 16)
    end

    local hash = bit.tobit(seed)
    local remainder = #key % 4

    for i = 1, #key - remainder, 4 do
        local k = struct.unpack('<I4', key, i)
        k = multiply(k, c1)
        k = bit.rol(k, r1)
        k = multiply(k, c2)
        hash = bit.bxor(hash, k)
        hash = bit.rol(hash, r2)
        hash = multiply(hash, m) + n
    end

    if remainder ~= 0 then
        local k1 = struct.unpack('<I' .. remainder, key, #key - remainder + 1)
        k1 = multiply(k1, c1)
        k1 = bit.rol(k1, r1)
        k1 = multiply(k1, c2)
        hash = bit.bxor(hash, k1)
    end

    hash = bit.bxor(hash, #key)
    hash = bit.bxor(hash, bit.rshift(hash, 16))
    hash = multiply(hash, 0x85ebca6b)
    hash = bit.bxor(hash, bit.rshift(hash, 13))
    hash = multiply(hash, 0xc2b2ae35)
    hash = bit.bxor(hash, bit.rshift(hash, 16))
    return hash > 0 and hash or 0x100000000 + hash
end

local function get_row(key, bucket_size, bits, index)
    local row = {}
    local offset = bucket_size * bits * index

    local row_start = math.floor(offset / 8)
    local row_end = math.ceil((offset + bits * bucket_size) / 8) - 1
    local row_str = redis.call("getrange", key, row_start + hdr_size, row_end + hdr_size)

    local bytes_per_cell = math.ceil(bits / 8)
    local leftover = math.fmod(offset, 8)
    local rtrim = 8 - math.fmod(bits, 8)

    local exp_len = math.ceil((bucket_size * bits + leftover)/8)
    assert(row_str:len() == row_end - row_start + 1, "bitfield unexpectedly short")
    assert(row_str:len() == exp_len, "incorrect row_str length: " .. row_str:len() .. "!=" .. exp_len)

    for i=1, bucket_size do
        local cell_str = row_str:sub(math.floor(leftover / 8) + 1, math.ceil((leftover + bits) / 8))
        local shift = math.fmod(leftover, 8)
        local cell_str_len = cell_str:len()
        local str = {}

        for j=1, cell_str_len-1 do
            table.insert(str, bit.band(bit.bor(bit.lshift(cell_str:byte(j), shift), bit.rshift(cell_str:byte(j+1), 8-shift)), bytemask))
        end
        table.insert(str, bit.band(bit.lshift(cell_str:byte(-1), shift), bytemask))

        local exp_cell_str_len = math.ceil((shift + bits) / 8)
        assert(cell_str:len() == exp_cell_str_len, "incorrect cell_str length: " .. cell_str:len() .. "!=" .. exp_cell_str_len)

        local last = str[bytes_per_cell]
        assert(last, "cannot extract last byte")
        str[bytes_per_cell] = bit.lshift(bit.rshift(last, rtrim), rtrim)

        str = string.char(unpack(str)):sub(1, bytes_per_cell)
        assert(str:len() == bytes_per_cell, "unexpected length")

        table.insert(row, str)
        leftover = leftover + bits
    end
    return row
end

local function is_empty(entry)
    for i,v in ipairs({entry:byte(1,-1)}) do
        if v ~= 0 then
            return false
        end
    end
    return true
end

local function hex2bin(str)
    return (str:gsub('..', function (cc)
        return string.char(tonumber(cc, 16))
    end))
end

local function fingerprint(x, bits)
    local res = hex2bin(redis.sha1hex(x)):sub(1, math.ceil(bits/8))
    local remainder = math.fmod(bits, 8)
    if remainder ~= 0 then
        local last_byte = res:sub(-1,-1):byte()
        res = res:sub(1,-2)
        last_byte = bit.rshift(last_byte, 8 - remainder)
        if last_byte == 0 and is_empty(res) then
            last_byte = 1
        end
        last_byte = bit.lshift(last_byte, 8 - remainder)
        res = res .. string.char(last_byte)
    else
        res = res:sub(1, bits / 8)
        if is_empty(res) then
            res = res:sub(1, -2) .. string.char(1)
        end
    end
    assert(res:len() == math.ceil(bits / 8), "incorrect resulting fingerprint length")
    return res
end

local function other_index(i1, f, modulus)
    local fh = hash32(f)
    local i2 = bit.band(bit.bxor(i1, fh), modulus-1)
    if i2 < 0 then
        i2 = i2 + modulus
    end
    return i2
end

local function cf_lookup(key, number_of_buckets, bucket_size, bits, elem)
    local f = fingerprint(elem, bits)
    local i1 = bit.band(hash32(elem), number_of_buckets - 1)
    local i2 = other_index(i1, f, number_of_buckets)

    local b1 = get_row(key, bucket_size, bits, i1)
    for bi, bv in ipairs(b1) do
        if bv == f then
            return true
        end
    end

    local b2 = get_row(key, bucket_size, bits, i2)
    for bi, bv in ipairs(b2) do
        if bv == f then
            return true
        end
    end
    
    -- element doesn't exists in table
    return false
end

assert(redis.replicate_commands(),
    "assertion failed - script oplog replication is not supported")

local key_name = KEYS[1]
assert(key_name, "Exactly one key must be specified")

local number_of_buckets, bits_per_fingerprint, bucket_size = unpack(
    redis.call("bitfield", key_name,
        "get", "u32", 0,
        "get", "u8",  0 + 32,
        "get", "u8",  0 + 32 + 8))

local res = {}
for arg_no, elem in ipairs(ARGV) do
    table.insert(res, cf_lookup(key_name, number_of_buckets, bucket_size, bits_per_fingerprint, elem))
end

return res
