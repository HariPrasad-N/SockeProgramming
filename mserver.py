import socket
import json
from threading import Thread 
from SocketServer import ThreadingMixIn
import pandas as pd
import time
import numpy as np

lock=1
#import pandas as pd

def sendtrip():
        MESSAGE=train.to_json(orient='split')
        conn.send(MESSAGE)
        return city.to_json(orient='split')
        
def addtrip(query):
        query=query.split(" ")
        last=getlast(trip,'trip')
        query.insert(0,last)
        insert(trip,query)
        display1=create()
        return display1.to_json(orient='split')
        
def insert(df,lis):
        row=df.shape[0]
        df.loc[row]=lis

def getlast(df,i):
        return int(df[i].iloc[-1])+1

def book(query):
        global lock
        while lock!=1:
                time.sleep(5)
        lock=0
        query=query.split(" ")
        trips=query[0]
        reqs=" ".join(query[1: ])
        reqs=json.loads(reqs)
        temp=seat.query("t_id == '"+trips+"'")
        seats=[]
        ids=[]
        for i in range(0,len(reqs),2):
                seats.append(reqs[i])
        if set(seats) <= set(temp['sno']):
                for i in range(0,len(reqs),2):
                        no=reqs[i]
                        id=getlast(passenger,'id')
                        ids.append(id)
                        reqs[i+1].insert(0,id)
                        insert(passenger,reqs[i+1])
                        temp=seat.query("t_id == '"+trips+"' & sno == '"+no+"'")
                        seat.loc[temp.index.tolist()[0]].pid=id
                lock=1
                detail=""
                for i in range(0,len(seats)):
                        detail=detail+" Seat "+str(seats[i])+" pid "+str(ids[i])+"\n"
                print detail
                return "Seats successfully booked \n"+detail
        else:
                lock=1
                return "Seats already booked"
        
        
def cancel(query):
        query=query.split(" ")
        trips=query[0]
        
def query(query):
        q=query.split(' ')
        query=' '.join(q[1: ])
        if q[0].lower()=='del':
                seat.loc[int(query)].pid=np.nan
                return "Successfully cancelled"
        elif q[0].lower()=='show1':
                result=seat.query(query)
                return result.to_json(orient='split')
        elif q[0].lower()=='search':
                return search(query)
        elif q[0].lower()=='show':
                return show(query)
        elif q[0].lower()=='at':
                return add_train(query)
        elif q[0].lower()=='cit':
                return add_city(query)
        elif q[0].lower()=='book':
                return book(query)
        elif q[0].lower()=='trip':
                return sendtrip()
        elif q[0].lower()=='atr':
                return addtrip(query)
                
def add_city(query):
    insert(city,[getlast(train,'id'),query])
    return city.to_json(orient='split')
    
def add_train(query):
    insert(train,[getlast(train,'id'),query])
    return train.to_json(orient='split')


def search(query):
        if query=='':
                return 'No results found'
        else:
                result=display1.query(query)
                return result.to_json(orient='split')
def show(query):
        if query=='':
                return 'No results found'
        else:
                result=seat.query(query)
                result=result[pd.isnull(result['pid'])]
                result=result[["sno","price","type"]]
                result.columns=['Seat','Price','Type']
                return result.to_json(orient='split')
                
def create():
        display1=trip.merge(train,left_on='t_id',right_on='id',how='inner').merge(city, left_on='s_id',right_on='id',how='inner').merge(city,left_on='d_id',right_on='id',how='inner')

        display1=display1[['trip','name_x','name_y','name','dt','at','date']]

        display1.columns=['Id','Train','From','To','Departure','Arrival','Date']
        return display1
        
#Multithreaded Python Server: TCP server Socket Thread Pool
class ClientThread(Thread):
        def __init__(self,ip,port):
                Thread.__init__(self)
                self.ip=ip
                self.port=port
                self._is_running=True
                print ("[+] New server socket thread started for " + ip + ":" + str(port))
        
        def run(self):
                conn.send(display1.to_json(orient='split'))
                while(self._is_running):
                        data=conn.recv(2048)
                        MESSAGE = query(data)
                        if(data=='exit'):
                                MESSAGE='exit'                          
                        if MESSAGE =='exit':
                                self._is_running = False
                                break
                        else:
                                conn.send(MESSAGE)
        
        
#Multithreaded Python Server: TCP Server Socket Program Stub
TCP_IP= '0.0.0.0'
TCP_PORT = 2011
BUFFER_SIZE = 20 #Usually 1024, but we need quick response

#loading the data frame
train=pd.read_csv("train.csv",skipinitialspace=True,dtype=str)
city=pd.read_csv("city.csv",skipinitialspace=True,dtype=str)
passenger=pd.read_csv("passenger.csv",dtype=str,skipinitialspace=True)
seat=pd.read_csv("seat.csv",dtype=str,skipinitialspace=True)
trip=pd.read_csv("trip.csv",skipinitialspace=True,dtype=str)
waiting=pd.read_csv("waiting.csv",skipinitialspace=True,dtype=str)


display1=create()


tcpServer =socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcpServer.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1)
tcpServer.bind((TCP_IP, TCP_PORT))
threads=[]

while True:
        tcpServer.listen(4)
        print ("Multithreaded Python Server : Waiting for connections from TCP clients...")
        (conn, (ip,port))=tcpServer.accept()
        newthread = ClientThread(ip,port)
        newthread.start()
        threads.append(newthread)

for t in threads:
        t.join()
        

