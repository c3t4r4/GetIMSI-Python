#!/usr/bin/env python3
#-*- coding: utf-8 -*-

#pip3 install watchdog

from ast import Not
import time, os, sqlite3
from datetime import datetime
from watchdog.observers import Observer  
from watchdog.events import PatternMatchingEventHandler

oldFileContent = ""
newLineContent = []
printLog = True

folderFiles = "./files/"
dbName = folderFiles+"IMSI.db"
dbCon = None
dbCursor = None

class imsi_Data:
    def __init__(self, type, datetimsi, operID, timsi, imsi, dateimsi, status):
        self.type = type
        self.datetimsi = datetimsi
        self.operID = operID
        self.timsi = timsi
        self.imsi = imsi
        self.dateimsi = dateimsi
        self.status = status

def printlog(text):
    if printLog:
        print(f"Debug: {text}")

def connectDB():
    global dbName
    global dbCon
    global dbCursor

    dbCon = sqlite3.connect(dbName)
    dbCursor = dbCon.cursor()

def closeDB():
    global dbName
    global dbCon
    global dbCursor

    if dbCon:
        dbCursor.close()
        dbCon.commit()
        dbCursor = None
        dbCon.close()

def createTable():
    global dbCursor

    connectDB()

    dbCursor.execute('''CREATE TABLE IF NOT EXISTS IMSI(ID INTEGER PRIMARY KEY AUTOINCREMENT,IMSI TEXT UNIQUE NOT NULL,CREATED_AT TEXT NOT NULL, UPDATED_AT DATE NOT NULL, STATUS INT NOT NULL);''')

    dbCursor.execute('''CREATE TABLE IF NOT EXISTS TIMSI(ID INTEGER PRIMARY KEY AUTOINCREMENT,IMSI_ID INTEGER NOT NULL,TIMSI TEXT UNIQUE NOT NULL,CREATED_AT TEXT NOT NULL, UPDATED_AT DATE NOT NULL, STATUS INT NOT NULL);''')

    dbCursor.execute('''CREATE TABLE IF NOT EXISTS LOCATED(ID INTEGER PRIMARY KEY AUTOINCREMENT,IMSI_ID INTEGER NOT NULL,TIMSI_ID INTEGER NOT NULL,CREATED_AT TEXT NOT NULL,UPDATED_AT DATE NOT NULL, STATUS INT NOT NULL);''')    

    closeDB()

def insertIMSI(objIMSI):
    global dbCursor

    imsiID = 0
    connectDB()

    stringSql = f"SELECT ID FROM IMSI WHERE IMSI='{str(objIMSI.imsi)}';"

    dbCursor.execute(stringSql)
    result = dbCursor.fetchone()

    if result is not None:
        imsiID = result[0]
    else:
        stringSql = f"INSERT OR IGNORE INTO IMSI (IMSI, CREATED_AT, STATUS, UPDATED_AT) VALUES ('{str(objIMSI.imsi)}', '{str(objIMSI.dateimsi)}', 0, datetime('now','localtime')) RETURNING ID;"
        dbCursor.execute(stringSql)
        
        if dbCursor.lastrowid > 0:
            imsiID = dbCursor.lastrowid

    closeDB()
    return imsiID

def insertTIMSI(imsiID, objIMSI):
    global dbCursor

    timsiID = 0

    if imsiID > 0:
        tmsi = objIMSI.timsi

        if tmsi != None:
            connectDB()
            stringSql = f"SELECT ID FROM TIMSI WHERE TIMSI='{str(tmsi)}';"

            dbCursor.execute(stringSql)
            result = dbCursor.fetchone()

            if result is not None:
                timsiID = result[0]
            else:
                stringSql = f"INSERT OR IGNORE INTO TIMSI (IMSI_ID, TIMSI, CREATED_AT, STATUS, UPDATED_AT) VALUES ({str(imsiID)}, '{str(tmsi)}', '{str(objIMSI.datetimsi)}', 0, datetime('now','localtime')) RETURNING ID;"

                dbCursor.execute(stringSql)
                    
                if dbCursor.lastrowid > 0:
                    timsiID = dbCursor.lastrowid
            closeDB()
    return timsiID

def insertLocated(imsiID, tmsiID, objIMSI):
    global dbCursor

    if imsiID > 0 and tmsiID > 0:
        connectDB()

        stringSql = f"SELECT ID FROM LOCATED WHERE IMSI_ID={str(imsiID)} AND TIMSI_ID={str(tmsiID)} AND CREATED_AT='{str(objIMSI.dateimsi)}';"

        dbCursor.execute(stringSql)
        result = dbCursor.fetchone()

        if result is None:
            stringSql = f"INSERT INTO LOCATED (IMSI_ID, TIMSI_ID, CREATED_AT, STATUS, UPDATED_AT) VALUES ({str(imsiID)}, {str(tmsiID)}, '{str(objIMSI.dateimsi)}', 0, datetime('now','localtime'));"
            dbCursor.execute(stringSql)

        closeDB()

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

class MyHandler(PatternMatchingEventHandler):
    patterns = ["*.txt"]

    def process(self, event):
        global dbCon

        if event.event_type == "created" or event.event_type == "modified":
            datamovimento = datetime.now().strftime('%Y%m%d%H%M%S')
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
                            imsiID = insertIMSI(objectLine)
                            timsiID = insertTIMSI(imsiID, objectLine)
                            insertLocated(imsiID, timsiID, objectLine)        

                print(f"Processando Arquivo!")
                print(event.src_path, event.event_type)  # print now only for degug
                            
                print(f"Arquivo Finalizado!\n")
                print(f"MicroServiço = Escuta IMSI File\n")

    def on_modified(self, event):
        self.process(event)

    def on_created(self, event):
        self.process(event)

if __name__ == '__main__':
    dttmpstatus = ""
    print("MicroServiço = Escuta IMSI File")

    createTable()
    
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