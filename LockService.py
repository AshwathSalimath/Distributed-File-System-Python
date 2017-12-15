import SocketServer
import json
import time

Host = "localhost"   #"127.0.0.1"
Port = 8884

LOCK_TIMEOUT = 30

LOCK_MAPPINGS = {}  #array that holds all the mapping of each locks --i.e which lock assigned to which client etc

def lockExists(filename):
    return filename in LOCK_MAPPINGS

def getLockMapping(filename):
    if lockExists(filename):
        return LOCK_MAPPINGS[filename]
    else:
        return None

def addLockMapping(filename, clientid, timestamp, timeout):
    LOCK_MAPPINGS[filename] = {"clientid": clientid, "timestamp": timestamp, "timeout": timeout}

def deleteLockMapping(filename):
    del LOCK_MAPPINGS[filename]

class ThreadedHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        msg = self.request.recv(1024)
        #print msg

        msg = json.loads(msg)
        requestType = msg['request']

        print ("Request type = " + requestType)

        response = ""

        # request checking if file is locked
        if requestType == "checklock":
            if lockExists(msg['filename']):
                print ("Check lock -> lock exists")
                timestamp = time.time()
                fs = getLockMapping(msg['filename'])

                # deletes lock if timed out
                if fs['timestamp']+fs['timeout'] < timestamp:
                    print ("Lock has timed out")
                    print (fs['timestamp']+fs['timeout'])
                    print (timestamp)

                    deleteLockMapping(msg['filename'])
                    response = json.dumps({
                        "response": "unlocked"
                    })
                # if client owns lock
                elif msg['clientid'] == fs['clientid']:
                    print ("Check lock -> lockowned")
                    response = json.dumps({
                        "response": "lockowned",
                        "filename": msg['filename'],
                        "timestamp": fs['timestamp'],
                        "timeout": fs['timeout']
                    })
                # if client does not own lock
                else:
                    print ("Check lock -> locked")
                    response = json.dumps({
                        "response": "locked",
                        "filename": msg['filename'],
                        "timestamp": fs['timestamp'],
                        "timeout": fs['timeout']
                    })
            else:
                response = json.dumps({
                    "response": "unlocked"
                })
        # request to obtain lock
        elif requestType == "obtainlock":
            if lockExists(msg['filename']):
                print ("Obtain lock -> lock exists")

                fs = getLockMapping(msg['filename'])
                timestamp = time.time()

                # deletes lock if timed out
                if fs['timestamp']+fs['timeout'] < timestamp:
                    print ("Obtain lock -> lock timed out, obtain again")

                    print (fs['timestamp']+fs['timeout'])
                    print (timestamp)

                    deleteLockMapping(msg['filename'])
                    addLockMapping(msg['filename'], msg['clientid'], timestamp, LOCK_TIMEOUT)

                    response = json.dumps({
                        "response": "lockgranted",
                        "filename": msg['filename'],
                        "timestamp": fs['timestamp'],
                        "timeout": fs['timeout']
                    })
                # if client owns lock
                elif msg['clientid'] == fs['clientid']:
                    print ("Check lock -> lockowned")
                    timestamp = time.time()
                    response = json.dumps({
                        "response": "lockregranted",
                        "filename": msg['filename'],
                        "timestamp": timestamp,
                        "timeout": LOCK_TIMEOUT
                    })
                # if client does not own lock
                else:
                    print ("Obtain lock -> locked already")
                    response = json.dumps({
                        "response": "locked",
                        "filename": msg['filename'],
                        "timestamp": fs['timestamp'],
                        "timeout": fs['timeout']
                    })
            # grants the lock for the timeout period
            else:
                print ("Obtain lock -> lock granted")
                timestamp = time.time()
                addLockMapping(msg['filename'], msg['clientid'], timestamp, LOCK_TIMEOUT)

                response = json.dumps({
                    "response": "lockgranted",
                    "filename": msg['filename'],
                    "clientid": msg['clientid'],
                    "timestamp": timestamp,
                    "timeout": LOCK_TIMEOUT
                })
        else:
            response = json.dumps({"response": "Error", "error": requestType+" is not a valid request"})

        self.request.sendall(response.encode('utf-8'))


class LockingServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass

if __name__ == '__main__':
    address = (Host, Port)
    server = LockingServer(address, ThreadedHandler)
    server.serve_forever()
