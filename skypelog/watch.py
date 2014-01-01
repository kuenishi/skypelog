import Skype4Py
import riak
import time
import json
import sys

#rc = riak.RiakClient(host="controller", port=8098)
rc = riak.RiakClient(host=sys.argv[1],port=8098)

skypelog_bucket = rc.bucket('skypelog')

def handle_ping(msg):
    msg.Chat.SendMessage('#pong')

#def handle_search(msg):
#    date = msg.Body[8:]
#    data = skypelog_bucket.get_index('DateTime_bin', date, return_terms=True)
#    print data


def handler(msg, event):
    """ msg is instance of chat.ChatMessage see chat.py """
    if msg.Body == '#ping': handle_ping(msg)
#    elif msg.Body[:8] == '#search ': handle_search(msg)

    if msg.Body == '#pong' and msg.FromHandle == 'kuenishi_bot': pass
    else: handle_msg(msg, event)

def handle_msg(msg, event):
    m = {}
    m['Body'] = msg.Body
    m['Topic'] = msg.Chat.Topic
    #print(msg.Chat.FriendlyName)
    #print(msg.Chat.Topic)
    #print(msg.Chat.Timestamp)
    #print(msg.Chat.Status)
    #print(msg.Chat.Datetime)
    m['ChatName'] = msg.Chat.Name
    m['DateTime'] = msg.Datetime.__str__()
    m['FromDisplayName'] = msg.FromDisplayName
    m['FromHandle'] = msg.FromHandle
    m['Id'] = msg.Id
    m['event'] = event
    key = msg.Chat.Name + '||' + str(msg.Id)
    log = skypelog_bucket.new(key, data=m)
    log.add_index('DateTime_bin', m['DateTime'])
    log.add_index('FromHandle_bin', m['FromHandle'])
    log.add_index('Id_int', m['Id'])
    log.add_index('event_bin', m['event'])
    log.add_index('ChatName_bin', m['ChatName'])
    log.store()

    print(json.dumps(m))
    if log.exists():
        print( "%s stored." % key )
        ## infinite loop msg.Chat.SendMessage('stored into riak: %s' % key)
    else:
        msg.Chat.SendMessage('failed to store data; riak seems dead')
    ## print(log.get_metadata())


if __name__ == '__main__':
    skype = Skype4Py.Skype()
    skype.Attach()
    skype.OnMessageStatus = handler
    while True:
        time.sleep(1)

    print('hoge')
