#!/usr/bin/env python

CF_INSERT='cf_insert.lua'
BULK_SIZE = 100

import sys
import os.path
import json
import redis
import itertools

LUA_BASE = os.path.realpath(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        '..'
    )
)

def usage():
    print >> sys.stderr, """Usage: %s <JSON with Redis connection options> <Cuckoo filter Redis key name>

Elements are read from stdin, each per line.""" % (sys.argv[0],)
    sys.exit(2)

def main():
    if len(sys.argv) != 3:
        usage()
    try:
        redis_opts = json.loads(sys.argv[1])
    except:
        usage()
    key_name = sys.argv[2]

    with open(os.path.join(LUA_BASE, CF_INSERT)) as sf:
        cf_insert_script = sf.read()

    r = redis.StrictRedis(**redis_opts)
    cf_insert_hash = r.script_load(cf_insert_script)
    counter = 0
    for k, g in itertools.groupby( ( (i, v.rstrip('\n')) for i, v in enumerate(sys.stdin)), lambda (k,v): k / BULK_SIZE):
        elems = list(elem for k, elem in g)
        res = r.evalsha(cf_insert_hash, 1, key_name, *elems)
        for e, c in itertools.izip(elems, res):
            if c != 1:
                print >> sys.stderr, "failed to insert element %s, code=%s" % (repr(e),repr(c))
        counter += 1
        if counter % 1000 == 0:
            print "%d batches processed." % (counter,)

if __name__ == '__main__':
    main()
