import SocketServer
import json
import uuid
import random

HOST = "localhost"#"192.168.0.1"
PORT = 8080

FILE_SERVERS = {}
FILE_MAPPINGS = {} #holds all details for files - id,address,port and timestamps

def fileExists(filename):
    return filename in FILE_MAPPINGS

def getFileMapping(filename):
    if fileExists(filename):
        return FILE_MAPPINGS[filename]
    else:
        return None

def addFileMapping(filename, nodeID, address, port, timestamp):
    FILE_MAPPINGS['filename'] = {"nodeID": nodeID, "address": address, "port": port, "timestamp": timestamp}

def deleteFileMapping(filename):
    del FILE_MAPPINGS[filename]

def getRandomServer():
    index = random.randint(0, len(FILE_SERVERS)-1)
    return FILE_SERVERS.items()[index]

class ThreadedHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        msg = self.request.recv(1024)
        #print msg

        msg = json.loads(msg)
        requestType = msg['request']

        response = ""

        if requestType == "open":
            if fileExists(msg['filename']):
                fs = getFileMapping(msg['filename'])
                response = json.dumps({
                    "response": "open-exists",
                    "filename": msg['filename'],
                    "isFile": True,
                    "address": fs['address'],
                    "port": fs['port'],
                    "timestamp": fs['timestamp']
                })
            else:
                fs = getRandomServer()
                response = json.dumps({
                    "response": "open-null",
                    "filename": msg['filename'],
                    "isFile": False,
                    "uuid": fs[0],
                    "address": fs[1]['address'],
                    "port": fs[1]['port']
                })
        elif requestType == "close":
            response = json.dumps({
                "response": "close",
                "filename": msg['filename'],
                "isFile": True
            })
        elif requestType == "read":
            if fileExists(msg['filename']):
                fs = getFileMapping('filename')
                response = json.dumps({
                    "response": "read-exists",
                    "filename": msg['filename'],
                    "isFile": True,
                    "address": fs['address'],
                    "port": fs['port'],
                    "timestamp": fs['timestamp']
                })
            else:
                response = json.dumps({
                    "response": "read-null",
                    "filename": msg['filename'],
                    "isFile": False
                })
        elif requestType == "write":
            print (msg['filename'])
            print (FILE_MAPPINGS)
            if fileExists(msg['filename']):
                print ("write if")
                fs = getFileMapping(msg['filename'])
                response = json.dumps({
                    "response": "write-exists",
                    "filename": msg['filename'],
                    "isFile": True,
                    "uuid": fs['uuid'],
                    "address": fs['address'],
                    "port": fs['port'],
                    "timestamp": msg['timestamp']
                })
            else:
                print ("write else")
                fs = getRandomServer()
                FILE_MAPPINGS[msg['filename']] = {"uuid": fs[0], "address": fs[1]['address'], "port": fs[1]['port'], "timestamp": msg['timestamp']}
                print (FILE_MAPPINGS)
                response = json.dumps({
                    "response": "write-null",
                    "filename": msg['filename'],
                    "isFile": False,
                    "uuid": fs[0],
                    "address": fs[1]['address'],
                    "port": fs[1]['port'],
                    "timestamp": msg['timestamp']
                })
        elif requestType == "dfsjoin":
            nodeID = msg['uuid']
            # if evaluates to True-- the file server is new and a uuid will be generated
            # if evaluates to False-- the file server exists and already has a uuid
            if(nodeID == ""):
                nodeID = str(uuid.uuid4())

            FILE_SERVERS[nodeID] = {"address": msg['address'], "port": msg['port']}
            response = json.dumps({"response": requestType, "uuid": nodeID})
            print (FILE_SERVERS)
        else:
            response = json.dumps({"response": "error", "error": requestType+" is not a valid request"})

        self.request.sendall(response.encode('utf-8'))


class MasterServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass

if __name__ == '__main__':
    address = (HOST, PORT)
    server = MasterServer(address, ThreadedHandler)
    server.serve_forever()
