#!/usr/bin/env python3
from socket import socket, gethostname
from time import sleep
import csv, json
import logging

folder_path="/home/zain/PythonProjects/PrimitivMetrics/"

def getConfig():
    with open(folder_path+'config.json', 'r') as f:
        config = json.load(f)
        return config

header_size = int(getConfig()['header_size'])
buffer_size = int(getConfig()['buffer_size'])
csv_output = getConfig()['csv_output']
output_field_names = getConfig()['output_field_names']
workers_input = getConfig()['workers_input']

def makeHeader(data):
    header = f'{len(data):<{header_size}}'
    return header

def sendData(this_socket, data):
    header = makeHeader(data)
    data = header+data
    this_socket.sendall(data.encode())

def readHeader(header):
    data_size = int(header.decode())
    return data_size

def recvData(this_socket):
    header = this_socket.recv(buffer_size)
    data_size = readHeader(header)

    full_data = ''
    data = this_socket.recv(buffer_size)
    full_data += data.decode()
    while len(full_data) - data_size != 0:
        data = this_socket.recv(buffer_size)
        full_data += data.decode()
    
    return full_data

def makeSocket():
    this_socket = socket()
    
    return this_socket

def connect(this_socket, ip, port):
    this_socket.connect((ip, port))

def getWorkers():
    with open(workers_input,'r') as f:
        return json.load(f)

def toCSV(data):
    with open(csv_output, 'a') as f:
        writer = csv.DictWriter(f, fieldnames = output_field_names)
        #writer.writeheader()
        writer.writerows(data)

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

def main():
    
    try:
        worker_list = getWorkers()
    except Exception as exception:
        maintain_log(log_entry = exception, type = "e")
    metrics = []
    for row in worker_list:
        this_socket = makeSocket()

        ip = worker_list[row]['ip']
        port = int(worker_list[row]['port'])

        connection_flag = True
        try:
            connect(this_socket=this_socket, ip=ip, port=port)
        except Exception as exception:
            maintain_log(log_entry=f'{ip} {exception}', type="e")
            connection_flag = False

        if connection_flag == True:
            maintain_log(log_entry=f'{ip} Connection successful', type="i")
            response = json.loads(recvData(this_socket))
            
            metrics.append(response)

    toCSV(metrics)
        
if __name__ == "__main__":
    main()

