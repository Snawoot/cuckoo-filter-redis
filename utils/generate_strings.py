#!/usr/bin/env python

import sys
import uuid

def usage():
    print >> sys.stderr, "Usage: %s <number of strings>" % (sys.argv[0],)
    sys.exit(2)

def main():
    if len(sys.argv) != 2:
        usage()
    try:
        num = int(sys.argv[1])
    except:
        usage()

    for i in xrange(num):
        print str(uuid.uuid4())

if __name__ == '__main__':
    main()
