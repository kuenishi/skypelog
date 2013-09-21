import Skype4Py
import riak
import time
import json
import sys

if __name__ == '__main__':

    rc = riak.RiakClient(protocol='pbc',
                         nodes=[{'host':sys.argv[1], 'pb_port':8087}])
    skypelog_bucket = rc.bucket('skypelog')
    for keys in skypelog_bucket.stream_keys():
        ##print(keys)
        for key in keys:
            print(">>> %s" % key)
            print(skypelog_bucket.get(key).data)
