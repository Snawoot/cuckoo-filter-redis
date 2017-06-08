cuckoo-filter-redis
===================

Set of Lua stored functions implementing [Cuckoo Filter](https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf) backed by Redis.

## Requirements

  * Redis 3.2 or newer

## Usage

All commands illustrated using `redis-cli` utility. Of course you may use EVAL/EVALSHA commands provided by Redis interface for your programming language.

### Initialize

Initialize Cuckoo Filter with 8388608 buckets with 4 elements containing 73-bit fingerprints, backed by redis key with name `cf`:

```
redis-cli --eval cf_init.lua cf , 8388608 4 73
```

Response: true upon success

### Insert

Add some elements:

```
redis-cli --eval cf_insert.lua cf , elem1 elem2 very_long_element_number_3
```

Response: true or false for each element upon success

### Lookup

Check for elements existence:

```
redis-cli --eval cf_lookup.lua cf , elem2 elem3
```

Response: true or false for each element if corresponding element found in table

### Delete

Delete element from Cuckoo Filter:

```
redis-cli --eval cf_delete.lua cf , elem1

```

Response: true or false for each element if corresponding element existed in table before.