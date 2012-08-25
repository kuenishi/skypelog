import Skype4Py
import riak
import time
import json

if __name__ == '__main__':

    rc = riak.RiakClient(host="controller", port=8098)
    skypelog_bucket = rc.bucket('skypelog')
    for k in skypelog_bucket.get_keys():
        print(k)
        print(skypelog_bucket.get(k).get_data())
