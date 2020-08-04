#!/usr/bin/env python3
from socket import socket, gethostname, gethostbyname
from os.path import getsize
from time import sleep
import psutil, json, logging
from datetime import datetime

folder_path="/home/zain/PythonProjects/PrimitivMetrics/worker/"

def getConfig():
    with open(folder_path+'config.json', 'r') as f:
        config = json.load(f)
        return config

port = int(getConfig()['port'])
header_size = int(getConfig()['header_size'])
buffer_size = int(getConfig()['buffer_size'])

def makeHeader(data):
    header = f'{len(data):<{header_size}}'
    return header

def sendData(conn, data):
    header = makeHeader(data)
    data = header+data
    conn.sendall(data.encode())

def readHeader(header):
    data_size = int(header.decode())
    return data_size

def recvData(conn):
    header = conn.recv(buffer_size)
    data_size = readHeader(header)

    full_data = ''
    data = conn.recv(buffer_size)
    full_data += data.decode()
    while len(full_data) - data_size != 0:
        data = conn.recv(buffer_size)
        full_data += data.decode()
    
    return full_data

def maintain_log(log_entry, type):
    logFile = folder_path + "application_log.log"
    logging.basicConfig(filename=logFile, filemode='a', format='%(asctime)s %(levelname)s:%(message)s', level="INFO")
    if type == "i":
        logging.info(log_entry)
    elif type == "w":
        logging.warning(log_entry)
    elif type == "e":
        logging.error(log_entry)
    else:
        pass

def makeSocket(port):
    this_socket = socket()
    this_socket.bind((gethostbyname(gethostname()), port))

    return this_socket

def listenAndConnect(this_socket, connections):
    this_socket.listen(connections)
    maintain_log(log_entry="Open to connection.", type="i")
    conn, addr = this_socket.accept()
    maintain_log(log_entry=f"{addr} connected.", type="i")

    return conn, addr

def sendMetrics(conn):
    metrics = {}
    metrics['datetime'] = str(datetime.now())
    metrics['ip'] = gethostbyname(gethostname())
    metrics['cpu_pc'] = psutil.cpu_percent()
    try:
        metrics['cpu_temp'] = psutil.sensors_temperatures()['acpitz'][0][1]
    except:
        metrics['cpu_temp'] = 0
    metrics['ram_pc'] = psutil.virtual_memory().percent
    metrics = json.dumps(metrics)
    sendData(conn, metrics)
    
def main():
    getConfig()
    try:
        this_socket = makeSocket(port)
    except Exception as exception:
        maintain_log(log_entry=exception, type="e")
    while True:
        try:
            conn, addr = listenAndConnect(this_socket=this_socket, connections=1)
        except Exception as exception:
            maintain_log(log_entry=exception, type="e")

        sendMetrics(conn)
        
        
    
if __name__ == "__main__":
    main()

