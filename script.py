import csv
import datetime
import MySQLdb
import sys
def csv_sql(cur):
    def read_file(file):
        reader=csv.reader(open(file,'r'))
        prev=""
        for line in reader:
            if (prev==""):
                prev=line[0]               
                if "\xef\xbb\xbf" in line[0]:
                    tllamada=datetime.datetime.strptime(line[0][3:], "%d/%m/%y %H:%M:%S")
                else:
                    tllamada=datetime.datetime.strptime(line[0], "%d/%m/%y %H:%M:%S")
                idLlamada=line[1].replace("(", "").replace(")", "")
                destino=line[3].replace("(", "").replace(")", "")
                estado=line[4]
                sonado=convertion_to_seconds(line[5])
                hablado=convertion_to_seconds(line[6])
                total=convertion_to_seconds(line[7])                
                if (line[8]!=""):
                    costo=float(line[8].replace(",","."))
                else:
                    costo=0.0
                razon=line[9].replace("(", "").replace(")", "")
            else:
                if(prev!="" and line[0]==""):
                    sonado=sonado + convertion_to_seconds(line[5])
                    hablado=hablado+convertion_to_seconds(line[6])
                    if line[7]!="":
                        total=total+convertion_to_seconds(line[7])
                    else:
                        total=total
                    if line[8]!="":
                        costo=float(line[8].replace(",","."))
                    else:
                        costo=0.0
                    razon=line[9].replace("(", "").replace(")", "")
                if (prev!="" and line[0]!=""):
                    insert_database(tllamada, idLlamada, destino, estado, sonado, hablado, total, costo, razon)
                    if "\xef\xbb\xbf" in line[0]:
                        tllamada=datetime.datetime.strptime(line[0][3:], "%d/%m/%y %H:%M:%S")
                    else:
                        tllamada=datetime.datetime.strptime(line[0], "%d/%m/%y %H:%M:%S")
                    idLlamada=line[1].replace("(", "").replace(")", "")
                    destino=line[3].replace("(", "").replace(")", "")
                    estado=line[4]
                    sonado=convertion_to_seconds(line[5])
                    hablado=convertion_to_seconds(line[6])
                    #Comprobacion del total
                    if line[7]!="":
                        total=convertion_to_seconds(line[7])
                    else:
                        total=0.0
                    if line[8]!="":
                        costo=float(line[8].replace(",","."))
                    else:
                        costo=0.0
                    razon=line[9].replace("(", "").replace(")", "")
            
    def insert_database(tllamda,idllamada,destino,estado,sonado,hablado,total,costo,razon):
        #print 'INSERT INTO metabase.prueba VALUES ({},"{}","{}","{}",{},{},{},{},"{}",default);'.format(tllamda,idllamada,destino,estado,sonado,hablado,total,costo,razon)
        sql = 'INSERT INTO metabase.prueba VALUES ("{}","{}","{}","{}",{},{},{},{},"{}",default);'.format(tllamda.strftime('%Y-%m-%d %H:%M:%S'),idllamada,destino,estado,sonado,hablado,total,costo,razon)
        a=cur.execute(sql)
        print a
    file=sys.argv[1]
    read_file(file)

def convertion_to_seconds(data ):
    segundo=datetime.datetime.strptime(data,  "%H:%M:%S")
    return segundo.second + 60*segundo.minute + 3600*segundo.hour 
    
#Programa principal
#Conexion BDD
db = MySQLdb.connect(host="10.100.100.130", #host, usualmente localhost
                     port=3306,             #Puerto
                     user="metabase",       #usuario
                     passwd="metabase",     #password
                     db="metabase")         #nombre de la base de datos
cur = db.cursor()
#------------------#
#Llamada a la funcion general donde estan las otras funciones
#------------------#
csv_sql(cur)
db.commit() 
db.close()