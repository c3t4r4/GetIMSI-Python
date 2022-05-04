#!/usr/bin/env python3
#-*- coding: utf-8 -*-

#pip3 install watchdog

import time, os, requests, json
from ast import Not
from datetime import datetime
from watchdog.observers import Observer  
from watchdog.events import PatternMatchingEventHandler

oldFileContent = ""
newLineContent = []
printLog = True
folderFiles = "./files/"
URI = "localhost"
URLAPI = f"http://{URI}:8000/api/datastore"

class imsi_Data:
    def __init__(self, type, datetimsi, operID, timsi, imsi, dateimsi, status):
        self.type = type
        self.datetimsi = datetimsi
        self.operID = operID
        self.timsi = timsi
        self.imsi = imsi
        self.dateimsi = dateimsi
        self.status = status

    def toJson(self):
        object = {
            "type": self.type,
            "datetimsi": self.datetimsi,
            "operID": self.operID,
            "timsi": self.timsi,
            "imsi": self.imsi,
            "dateimsi": self.dateimsi,
            "status": self.status,
        }
        return json.dumps(object)

def printlog(text):
    if printLog:
        print(f"Debug: {text}")

def file_get_contents(filename):
    if os.path.exists(filename):
        fp = open(filename, "r")
        content = fp.read()
        fp.close()
        return content

def getObject(lineContent):

    respObj = None

    if lineContent:
        dadosIMSI = lineContent.split(";")
        printlog(dadosIMSI)
        if len(dadosIMSI) > 5:
            respObj = imsi_Data(dadosIMSI[0], dadosIMSI[1], dadosIMSI[2], dadosIMSI[3], dadosIMSI[4], dadosIMSI[5], dadosIMSI[6])
    return respObj

def getlistimsi(fileContent):
    global newLineContent
    listNewIMSI = []

    listaIMSI = fileContent.splitlines()

    for lineIMSI in listaIMSI:
        if lineIMSI not in newLineContent:
            newLineContent.append(lineIMSI)
            listNewIMSI.append(lineIMSI)

    return listNewIMSI

def sendJsonObject(imsiObject):
    header = {
        "Content-Type": "application/json"
    }

    jsonData = imsiObject.toJson()
    printlog(jsonData)
    response = requests.request("POST", URLAPI, data=jsonData, headers=header)
    print(f"        Status do Envio: {str(response.status_code)} \n")

class MyHandler(PatternMatchingEventHandler):
    patterns = ["*.txt"]

    def process(self, event):
        global dbCon

        if event.event_type == "created" or event.event_type == "modified":
            source = event.src_path
            global oldFileContent

            fileContent = file_get_contents(source)
            if oldFileContent != fileContent:
                oldFileContent = fileContent
                listNewIMSI = getlistimsi(fileContent)
                if listNewIMSI:
                    for lineObject in listNewIMSI:
                        objectLine = getObject(lineObject)
                        if objectLine:
                            sendJsonObject(objectLine)      

                print(f"Processando Arquivo!")
                print(event.src_path, event.event_type)  # print now only for degug
                            
                print(f"Arquivo Finalizado!\n")
                print(f"MicroServiço = Escuta IMSI File\n")
                printlog(f"Run on: {URLAPI}\n")

    def on_modified(self, event):
        self.process(event)

    def on_created(self, event):
        self.process(event)

if __name__ == '__main__':
    dttmpstatus = ""
    print("MicroServiço = Escuta IMSI File")
    printlog(f"Run on: {URLAPI}\n")
    
    args = folderFiles+"epc_imsi.txt"

    observer = Observer()
    observer.schedule(MyHandler(), path=args if args else '.')
    observer.start()

    try:
        while True:
            time.sleep(1)
            result = 1
            if result == True:
                dttmpstatus = datetime.today().strftime('%M')
    except KeyboardInterrupt:
        observer.stop()

    observer.join()