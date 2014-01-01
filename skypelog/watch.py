import Skype4Py
import riak
import time
import json
import sys
import urllib2

#rc = riak.RiakClient(host="controller", port=8098)
host = sys.argv[1]
rc = riak.RiakClient(host=sys.argv[1],pb_port=8098)

skypelog_bucket = rc.bucket('skypelog')
sandbox_bucket = rc.bucket('sandbox')

def handle_ping(msg):
    msg.Chat.SendMessage('#pong')

#def handle_search(msg):
#    date = msg.Body[8:]
#    data = skypelog_bucket.get_index('DateTime_bin', date, return_terms=True)
#    print data


def unicode_to_hex(s):
    return ''.join(hex(ord(i)) for i in s)

def handle_riak(msg, command):
    c = command
    if command is None: c = 'ping'
    elif command[:7] == 'buckets/skypelog':
        msg.Chat.SendMessage("don't do query!!")
    else:
        commands = command.split(' ')
        if commands[0] == 'help':
            msg.Chat.SendMessage('''
#riak help
#riak ping
#riak stats
#riak put <key> <data>
#riak get <key>
#riak delete <key>
''')
        elif commands[0] == 'put':
            if len(commands) > 2:
                key = unicode_to_hex(commands[1])
                k = sandbox_bucket.new(key)
                k.encoded_data=' '.join(commands[2:]).encode('utf8')
                k.charset='utf8'
                try:
                    msg.Chat.SendMessage(k.store())
                except TypeError as e:
                    msg.Chat.SendMessage(e)

        elif commands[0] == 'get':
            if len(commands) > 1:
                key = unicode_to_hex(commands[1])
                obj = sandbox_bucket.get(key)
                msg.Chat.SendMessage(obj.encoded_data)
        elif commands[0] == 'delete':
            if len(commands) > 1:
                key = unicode_to_hex(commands[1])
                obj = sandbox_bucket.get(key)
                obj.delete()
                msg.Chat.SendMessage('deleted')
        else:
            res = urllib2.urlopen('http://%s:8098/%s' % (host, c)).read()
            msg.Chat.SendMessage(res)

def handler(msg, event):
    """ msg is instance of chat.ChatMessage see chat.py """
    if   msg.Body == '#ping':      handle_ping(msg)
    elif msg.Body == '#riak':      handle_riak(msg, None)
    elif msg.Body[:6] == '#riak ': handle_riak(msg, msg.Body[6:])
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
    if log.exists:
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
