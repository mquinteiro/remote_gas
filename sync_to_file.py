#!/usr/bin/python3.5

from time import sleep
import pyodbc
import os
from subprocess import call

import numbers
import datetime
from google.cloud import storage
from google.cloud import pubsub
from cep_credentials import con_string, gce_cert_json_name



# Upload file to GCS
def upload_gcs(file_name):
    storage_client = storage.Client.from_service_account_json(gce_cert_json_name)
    bucket = storage_client.bucket('cepsa_shares')
    blob = bucket.blob(file_name)
    blob.upload_from_filename(file_name)
    # notify to pubsub
    topic = 'projects/mquinteiro/topics/cepsa_sync'
    publisher = pubsub.PublisherClient.from_service_account_json(gce_cert_json_name)
    publisher.publish(topic, file_name.encode('utf-8')).result()
    


  
def syncTable_strings(table="Telemedidas"):
    result_strings = [f"delete from {table};"]
    pyodbc.drivers()
    cnxn = pyodbc.connect(con_string)
    cursor = cnxn.cursor()
    cursor.execute("select * from " + table)
    first = True
    columns = ','.join([column[0] for column in cursor.description])
    for row in cursor.fetchall():
        query = "replace " + table + " ("+columns+")  values ("
        for i in range(0, len(row)):
            if i > 0:
                query += ','
            if type(row[i]) is int:
                query += str(row[i])
            elif row[i] is not None:
                query += '"' + row[i].replace('"', '\\"') + '"'
            else:
                query += "NULL"
        query += ');'
        # print(query)
        result_strings.append(query)
    return result_strings


def sync_DepositosAux_strings(max_id):
    result_strings = []
    pyodbc.drivers()
    cnxn = pyodbc.connect(con_string)
    cursor = cnxn.cursor()
    sql_query = "select DepositosAux.* from DepositosAux t, (select distinct key_data, key2_data from sync_updates where table_name='DepositosAux' and id<%s) s where t.CCanalizado=s.key_data and t.CodDep=s.key2_data" %max_id
    cursor.execute(sql_query)
    query = ""
    first = True
    columns = ','.join([column[0].split('.')[-1] for column in cursor.description])
    for row in cursor.fetchall():
        if not first:
            query += ',('
        else:
            "replace into DepositosAux (%s)values (" %columns
            first = False
        # print(row)
        for i in range(0, len(row)):
            if i > 0:
                query += ','
            if isinstance(row[i], numbers.Number):
                query += str(row[i])
            elif type(row[i]) is datetime.datetime:
                query += "'" + row[i].strftime("%Y-%m-%d %H:%M:%S") + "'"
            elif type(row[i]) is datetime.date:
                query += row[i]
            elif row[i] is not None:
                query += '"' + row[i] + '"'
            else:
                query += "NULL"
        query += ')'
        #print(query)
    if query:
        query += ';'
        result_strings.append(query)
    return result_strings

def sync_telemedidas_strings(max_id):
    result_strings = []
    pyodbc.drivers()
    cnxn = pyodbc.connect(con_string)
    cursor = cnxn.cursor()
    sql_query = "select Telemedidas.* from Telemedidas t, (select distinct key_data, from sync_updates where table_name='Telemedidas' and id<%s) s where t.NSerie=s.key_data" %max_id
    cursor.execute(sql_query)
    query = ""
    first = True
    columns = ','.join([column[0].split('.')[-1] for column in cursor.description])
    for row in cursor.fetchall():
        if not first:
            query += ',('
        else:
            "replace into Telemedidas (%s)values (" %columns
            first = False
        # print(row)
        for i in range(0, len(row)):
            if i > 0:
                query += ','
            if isinstance(row[i], numbers.Number):
                query += str(row[i])
            elif type(row[i]) is datetime.datetime:
                query += "'" + row[i].strftime("%Y-%m-%d %H:%M:%S") + "'"
            elif type(row[i]) is datetime.date:
                query += row[i]
            elif row[i] is not None:
                query += '"' + row[i] + '"'
            else:
                query += "NULL"
        query += ')'
        #print(query)
    if query:
        query += ';'
        result_strings.append(query)
    return result_strings

def syncLecuturas_strings(fecha):
    updated=0
    result_strings = []
    pyodbc.drivers()
    cnxn = pyodbc.connect(con_string)
    cursor = cnxn.cursor()
    cursor.execute("select count(*) from Lecturas where HorLectura>\'" + fecha.strftime("%Y-%m-%d %H:%M") + "\'")
    # for row in cursor.fetchall():
    #     print(row)
    # return
    cursor.execute("select * from Lecturas where HorLectura>\'" + fecha.strftime("%Y-%m-%d %H:%M") + "\'")
    query = "replace into Lecturas values ("
    first = True
    for row in cursor.fetchall():
        if not first:
            query += ',('
        else:
            first = False
        # print(row)
        for i in range(0, len(row)):
            if i > 0:
                query += ','
            if isinstance(row[i], numbers.Number):
                query += str(row[i])
            elif type(row[i]) is datetime.datetime:
                query += "'" + row[i].strftime("%Y-%m-%d %H:%M:%S") + "'"
            elif type(row[i]) is datetime.date:
                query += row[i]
            elif row[i] is not None:
                query += '"' + row[i] + '"'
            else:
                query += "NULL"
        query += ')'
        #print(query)
        updated+=1
    print("Numero de registros modificados = ",updated)
    if updated > 0:
        query += ';'
        result_strings.append(query)
    return result_strings
    #log = open("query01.log", "a+")
    #log.write ("getLastDate --> %s \r\n" %fecha)
    #log.write ("Query:-------------------------- \r\n %s \r\n" %query)

def main():
    #call(["/usr/sbin/vpnc", "test"])
    log = open("gasSync.log", "a+")
    try:
        last_sync_date = None
        if os.path.exists("last_sync.txt"):
            with open("last_sync.txt", "r") as last_sync:
                    last_sync_date = last_sync.read()
        if last_sync_date:
            last_sync_date = datetime.datetime.strptime(last_sync_date, "%Y-%m-%d %H:%M:%S.%f")
        else:
            last_sync_date = datetime.datetime(2021, 5, 12 ,10, 43, 27) 

        print(last_sync_date)

        ahora= datetime.datetime.now()
        strings = []
        file_name = ahora.strftime("%Y%m%d%H%M%S_cepsa_update.txt") 
        print(file_name)
        
        log.write (f"***Sync_Inicio: {datetime.datetime.now}\r\n")
        #syncTable("DepositosAux") #jfp 7/3/19 Ya no Sincronzamo DAux 
        #ahora= datetime.datetime.now()
        #log.write ("Sync_DepositosAux: %s \r\n" %ahora)
        #strings += syncTable_strings("DepositosAux")
        #strings += syncTable_strings("Telemedidas")
        log.write (f"end Sync_Telemedidas: {datetime.datetime.now}\r\n")
        strings += syncLecuturas_strings(last_sync_date)
        log.write (f"end Sync_Lecturas: {datetime.datetime.now}\r\n")
        
        with open(file_name, 'w') as f:
            for item in strings:
                f.write("%s\r\n" % item)
        last_sync = open("last_sync.txt", "w")
        last_sync.write(str(ahora))
        last_sync.close()
        log.write (f"start gce upload: {datetime.datetime.now}\r\n")
        upload_gcs(file_name)
        log.write (f"***Sync_End: {datetime.datetime.now}\r\n")
    finally:
        #call(["/usr/sbin/vpnc-disconnect"])
        log.write (f"******** SYNC_FAILURE: {datetime.datetime.now}\r\n")
        log.close()
        sleep(5)


if __name__ == '__main__':
	main()
