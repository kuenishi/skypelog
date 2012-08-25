import Skype4Py
import riak
import time
import json

rc = riak.RiakClient(host="controller", port=8098)
skypelog_bucket = rc.bucket('skypelog')

def handler(msg, event):
    """ msg is instance of chat.ChatMessage see chat.py """
    m = {}
    m['Body'] = msg.Body
    m['Topic'] = msg.Chat.Topic
    m['Name'] = msg.Chat.Name
    m['DateTime'] = msg.Datetime.__str__()
    m['FromDisplayName'] = msg.FromDisplayName
    m['FromHandle'] = msg.FromHandle
    m['Id'] = msg.Id
    m['event'] = event
    print(json.dumps(m))
    key = msg.Chat.Name + '||' + str(msg.Id)
    log = skypelog_bucket.new(key, data=m)
    log.store()
    if log.exists(): print( "%s stored." % key )
    print(log.get_metadata())
    print("\n")

if __name__ == '__main__':
    skype = Skype4Py.Skype()
    skype.Attach()
    skype.OnMessageStatus = handler
    while True:
        time.sleep(1)

    print('hoge')
