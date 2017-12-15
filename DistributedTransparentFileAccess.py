import SocketServer
import socket
import json
import os


NODEID = ""
HOST = "localhost"#"192.168.0.1"
PORT = 0 # automatically assigned

MASTER_ADDRESS = "localhost"#"127.0.0.1"
MASTER_PORT = 8080

CURRENT_DIRECTORY = os.getcwd()
print (CURRENT_DIRECTORY)
BUCKET_NAME = "DFSystem"              #directory to store files
BUCKET_PATH = os.path.join(CURRENT_DIRECTORY, BUCKET_NAME)

def dfsOpen(filename):
    path = os.path.join(BUCKET_PATH, filename)
    exists = os.path.isfile(path)
    return exists

def dfsRead(filename):
    path = os.path.join(BUCKET_PATH, filename)
    file_handle = open(path, "r")
    data = file_handle.read()
    return data

def dfsWrite(filename, data):
    path = os.path.join(BUCKET_PATH, filename)
    file_handle = open(path, "w+")
    file_handle.write(data)

class ThreadedHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        msg = self.request.recv(1024)
        print (msg)

        msg = json.loads(msg)
        requestType = msg['request']
        response = ""

        if requestType == "open":
            exists = dfsOpen(msg['filename'])
            response = json.dumps({"response": requestType, "filename": msg['filename'], "isFile": exists, "address": HOST, "port": PORT})
        elif requestType == "close":
            response = json.dumps({"response": requestType, "address": HOST, "port": PORT})
        elif requestType == "read":
            data = dfsRead(msg['filename'])
            response = json.dumps({"response": requestType, "address": HOST, "port": PORT, "data": data})
        elif requestType == "write":
            dfsWrite(msg['filename'], msg['data'])
            response = json.dumps({"response": requestType, "address": HOST, "port": PORT, "uuid": NODEID})
        else:
            response = json.dumps({"response": "Error", "error": requestType+" is not a valid request", "address": HOST, "port": PORT})

        self.request.sendall(response)


class FileServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass

if __name__ == '__main__':
    address = (HOST, PORT)
    server = FileServer(address, ThreadedHandler)
    PORT = server.socket.getsockname()[1]

    msg = json.dumps({"request": "dfsjoin", "uuid": NODEID, "address": HOST, "port": PORT})

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((MASTER_ADDRESS, MASTER_PORT))
    sock.sendall(msg.encode('utf-8'))
    response = sock.recv(1024)
    sock.close()

    data = json.loads(response)
    NODEID = data['uuid']

    print("File Server " + NODEID + " is listening on " + HOST + ":" + str(PORT))

    server.serve_forever()
