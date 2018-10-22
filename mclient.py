import socket
import pandas as pd
import json

def is_json(myjson):
        try:
                json_object = json.loads(myjson)
        except ValueError, e:
                return False
        return True

def cancel():
        trip=raw_input("Enter trip id or exit: ")
        if trip == 'exit':
                return "No"
        elif trip.isdigit():
                send="Show1 t_id == '"+trip+"'"
                tcpClientA.send(send)
                data = tcpClientA.recv(BUFFER_SIZE)
                df=pd.read_json(data,orient='split',dtype=str)
                if df.shape[0]==0:
                        print("No results found try again")
                else:
                        pa=raw_input("Enter seat number and passenger id : ")
                        pa=pa.split(" ")
                        if len(pa)==2:
                                df1=df.query("sno == '"+pa[0]+"' & pid == '"+pa[1]+"'")
                                if df1.shape[0]== 0:
                                        print("No passenger found try again")
                                else:
                                        return "del "+str(df1.index.tolist()[0])
                        else:
                                print("Wrong input")
        else:
                print("Wrong input")
                                             
def add():
        msg = raw_input('\t\t1.Add train\n\t\t2.Add city\n\t\t3.Add trip\n')
        msg = msg.lower()
        print msg
        if msg == "1" or msg == "add train":
                t = raw_input ("Enter train name:")
                return "at "+ t
        elif msg == "2" or msg =="add city":
                c = raw_input ("Enter city:")
                return "cit "+ c
        elif msg == "3" or msg =="add trip":
                tcpClientA.send("trip ")
                city=tcpClientA.recv(BUFFER_SIZE)
                train=tcpClientA.recv(BUFFER_SIZE)
                train=pd.read_json(train,orient='split',convert_dates=False)
                city=pd.read_json(city,orient='split',convert_dates=False)
                print(train)
                print(city)
                t = raw_input ("Enter train name source id destionation id arrival time departure time and date: ")
                return "atr "+ t
        else:
                return "No"
        
def book():
        while True:
                trip=raw_input("Enter the trip id or exit: ")
                if trip=='exit':
                        return 'No'
                elif trip.isdigit():
                        send="Show t_id == '"+trip+"'"
                        tcpClientA.send(send)
                        data = tcpClientA.recv(BUFFER_SIZE)
                        df=pd.read_json(data,orient='split',convert_dates=False,dtype=str)
                        if df.shape[0]==0:
                                print("No results found try again")
                        else:
                                print(df.to_string(index=False))
                                seats=raw_input("Enter seats : ")
                                seats=seats.split(" ")
                                list=[]
                                if set(seats) <= set(df['Seat']):
                                        for seat in seats:
                                                list.append(seat)
                                                name=raw_input("Enter name age gender for seat"+seat+" : ")
                                                name=name.split(" ")
                                                list.append(name)
                                        list=json.dumps(list)
                                        return "book "+trip+" "+list
                                else:
                                        print("No seat available")
                                
                                
                else:
                        print("Wrong input")
      

        
def query():
                message=raw_input("1. Search\n2. Book\n3. Cancel\n4. Add\n5. Exit\n")
                message=message.lower()
                if message == "1" or message == "search":
                                From=raw_input("From: ")
                                To=raw_input("To: ")
                                Date=raw_input("Date: ")
                                if From == '' :
                                        if To == '':
                                                if Date == '':
                                                        return 'Search '
                                                else:
                                                        return "Search Date == '"+Date+"'"
                                        else:
                                                if Date == '':
                                                        return "Search To == '"+To+"'"
                                                else:
                                                        return "Search To == '"+To+"' & Date == '"+Date+"'"
                                else:
                                        if To == '':
                                                if Date == '':
                                                        return "Search From == '"+From+"'"
                                                else:
                                                        return "Search From == '"+From+"' & Date == '"+Date+"'"
                                        else:
                                                if Date == '':
                                                        return "Search From == '"+From+"' & To == '"+To+"'"
                                                else:
                                                        return "Search From == '"+From+"' & To == '"+To+"' & Date == '"+Date+"'"
                                                
                                                
                elif message == "2" or message == "book":
                        return book()
                elif message == "3" or message == "cancel":
                        return cancel()
                elif message == "4" or message == "add":
                        return add()
                elif message == "5" or message == "exit":
                        return "exit"
                else:
                        print("Enter proper command")


host= socket.gethostname()
port = 2011
BUFFER_SIZE= 2000

 
tcpClientA = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcpClientA.connect((host,port))

data = tcpClientA.recv(BUFFER_SIZE)

print(pd.read_json(data,orient='split',convert_dates=False))

MESSAGE = query()

while MESSAGE != 'exit': 
        print MESSAGE 
        if MESSAGE!='No':
                tcpClientA.send(MESSAGE)
                data = tcpClientA.recv(BUFFER_SIZE)
                if(data=='exit'):
                        break
                if is_json(data):
                        df=pd.read_json(data,orient='split',convert_dates=False)
                        if df.shape[0]==0:
                                df="No results"
                else:
                        df=data
                print(df)
        MESSAGE = query()
        

tcpClientA.send(MESSAGE)
tcpClientA.close()

        
