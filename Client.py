import socket
import json
import uuid
import time
from io import StringIO

Main_Host = "localhost"#"127.0.0.1"
Main_Port = 8080
Locking_Host = "localhost"#"127.0.0.1"
Locking_Port = 8883

class Client():
    def __init__(self, masterHost, masterPort, lockHost, lockPort):
        self.id = str(uuid.uuid4())
        self.masterAddr = masterHost
        self.masterPort = masterPort
        self.lockAddr = lockHost
        self.lockPort = lockPort
        self.cache = {}

    def open(self, filename):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.masterAddr, self.masterPort))

        msg = json.dumps({"request": "open", "filename": filename, "clientid": self.id})
        sock.sendall(msg.encode('utf-8'))
        response = sock.recv(1024)

        return response

    def close(self, filename):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.masterAddr, self.masterPort))

        msg = json.dumps({"request": "close", "filename": filename, "clientid": self.id})
        sock.sendall(msg.encode('utf-8'))
        response = sock.recv(1024)
        return response

    def checkLock(self, filename):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.lockAddr, self.lockPort))

        msg = json.dumps({"request": "checklock", "filename": filename, "clientid": self.id})
        sock.sendall(msg.encode('utf-8'))
        response = sock.recv(1024)

        return response

    def obtainLock(self, filename):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.lockAddr, self.lockPort))

        msg = json.dumps({"request": "obtainlock", "filename": filename, "clientid": self.id})
        sock.sendall(msg.encode('utf-8'))
        response = sock.recv(1024)

        return response

    def read(self, filename):
        fileServerInfo = json.loads(self.open(filename))

        # check if file exists by checking with the directory server
        if fileServerInfo['isFile']:
            # check if file is in cache and its timestamp with the last write to the file--is it new or the same?
            if (filename in self.cache) and (self.cache[filename]['timestamp'] >= fileServerInfo['timestamp']):
                cacheFileInfo = self.cache[filename]
                print("Read '" + filename + "' from cache!")
                return cacheFileInfo
            else:
                addr = fileServerInfo['address']
                port = int(fileServerInfo['port'])

                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((addr, port))

                msg = json.dumps({"request": "read", "filename": filename, "clientid": self.id})
                sock.sendall(msg.encode('utf-8'))

                response = sock.recv(1024)

                self.cache['filename'] = json.loads(response)

                return response
        else:
        	return (filename + " does not exist!")

    # flow of the program----
    # 1. check the master server for location on data nodes
    # 2. check whether  file is locked by someone else or the current  client
    # 3. if not locked then write the file, else only write if current client owns the lock
    def write(self, filename, data):
        lockcheck = json.loads(client.checkLock(filename))

        if lockcheck['response'] == "locked":
            return "Cannot write as file is locked by another client!"

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.masterAddr, self.masterPort))

        timestamp = time.time()

        msg = json.dumps({"request": "write", "filename": filename, "clientid": self.id, "timestamp": timestamp})
        sock.sendall(msg.encode('utf-8'))
        response = sock.recv(1024)

        fileServerInfo = json.loads(response)

        addr = fileServerInfo['address']
        port = int(fileServerInfo['port'])

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((addr, port))

        content = {"request": "write", "filename": filename, "data": data, "clientid": self.id, "timestamp": timestamp}

        self.cache[filename] = content

        msg = json.dumps(content)
        sock.sendall(msg.encode('utf-8'))

        response = sock.recv(1024)
        return response


# client test
if __name__ == '__main__':
    client = Client(Main_Host, Main_Port,Locking_Host, Locking_Port)

    requestType = ""
    response = ""

    while requestType != "quit":
        requestType = raw_input("Please enter a request type eg: open - close - checklock - obtainlock - read - write or type quit to terminate the program: ")

        if requestType == "open":
            filename = raw_input("Please enter the filename: ")
            response = client.open(filename)
        elif requestType == "close":
            filename = raw_input("Please enter the filename: ")
            response = client.close(filename)
        elif requestType == "checklock":
            filename = raw_input("Please enter the filename: ")
            response = client.checkLock(filename)
        elif requestType == "obtainlock":
            filename = raw_input("Please enter the filename: ")
            response = client.obtainLock(filename)
        elif requestType == "read":
            filename = raw_input("Please enter the filename: ")
            response = client.read(filename)
        elif requestType == "write":
            filename = raw_input("Please enter the filename: ")
            data = raw_input("Please enter the file contents to write: ")
            response = client.write(filename, data)
        elif requestType == "quit":
            response = "Exiting Distributed File System!"

        else:
            response = "Not a valid request type, please try again."

        print (response)
