# load csv separated by | file into sql server tables canalizados and telemedidas.
# csv file is generated by sap ant it is in the credentials.py file as sap_csv_file

import chunk
from curses import keyname
from curses.ascii import isalnum
from ntpath import join
from cep_credentials import con_string, sftp_host, sftp_pro_user, sftp_pro_password
import pysftp
from time import sleep
from datetime import datetime
import pyodbc
import os
import csv

from generate_sap_linux import linux_connect


# for telemedidas (NSerial, CodigoSAPTel, CCanalizado, Telefono, Deposito1, Cod' +
#        'Dep1, Volumen1, Deposito2, CodDep2, Volumen2, Deposito3, CodDep3' +
#        ', Volumen3, Status)

fields_in_Telemedidas = ['NSerial', 'CodigoSAPTel', 'CCanalizado', 'Telefono', 'Deposito1', 'CodDep1', 'Volumen1',
                         'Deposito2', 'CodDep2', 'Volumen2', 'Deposito3', 'CodDep3', 'Volumen3', 'Status']
fields_for_Telemedidas = fields_in_Telemedidas

# field for Canalizados  (Codigo, Nombre, Localidad, CodPostal, Agencia, Transportista, Mantenedor, Status)
fields_in_Canalizados = ['Codigo', 'Nombre', 'Localidad', 'CodPostal', 'Agencia', 'Transportista', 'Mantenedor',
                          'Status']
fields_for_Canalizados = ['CCanalizado', 'Nombre', 'Localidad', 'CodPostal', 'Agencia', 'Transportista', 'Mantenedor', 'Status']

def loadCSVFile(filename):
    f = open(filename, 'r', encoding='ISO-8859-15') # windows
    # f = open(filename, 'r', encoding='ASCII') # linux
    SAP_field_names = ['empty','NSerial', 'Telefono', 'CodigoSAPTel', 'CCanalizado', 'Nombre', 'Localidad', 'CodPostal', 'Agencia', 
        'Transportista', 'Mantenedor', 'Status', 'E1', 'CodDep1', 'Volumen1', 'Deposito1', 
        'CodDep2', 'Volumen2', 'Deposito2', 'CodDep3', 'Volumen3', 'Deposito3']
    
    reader = csv.DictReader(f, SAP_field_names,delimiter='|',)
    data = {}
    for row in reader:
        del row['empty']
        if row['Deposito3'] is None:
            print("Error in row: ", row['NSerial'])
            continue
        data[row['NSerial']] = row
        data[row['NSerial']]['Volumen1'] = int(data[row['NSerial']]['Volumen1'].replace('.','')) if data[row['NSerial']]['Volumen1'].replace('.','').isnumeric()  else 0
        data[row['NSerial']]['Volumen2'] = int(data[row['NSerial']]['Volumen2'].replace('.','')) if data[row['NSerial']]['Volumen2'].replace('.','').isnumeric()  else 0
        data[row['NSerial']]['Volumen3'] = int(data[row['NSerial']]['Volumen3'].replace('.','')) if data[row['NSerial']]['Volumen3'].replace('.','').isnumeric()  else 0
        data[row['NSerial']]['Deposito3'] = data[row['NSerial']]['Deposito3'][:10]

    return data

# to get the file telemedidas.txt from sftp server, dat directory and save it as filename

def getSAPFile(filename):
    sftp = pysftp.Connection(host=sftp_host, username=sftp_pro_user, password=sftp_pro_password)
    sftp.get('dat/Telemedidas.txt', filename)
    sftp.close()

def main():
    today = datetime.now().strftime('%Y%m%d%H%M%S')
    sap_f_name = f'telemedidas_{today}.txt'
    getSAPFile(sap_f_name)
    data = loadCSVFile(sap_f_name)
    cur, con  = linux_connect()
    cur.execute("set names 'utf8'")
    cur.execute("set CHARACTER set  'utf8'")
    sql_str = 'delete from Telemedidas_SAP'
    cur.execute(sql_str)
    sql_str = 'delete from Canalizados'
    cur.execute(sql_str)
    sql_str_telemedidas = ""
    sql_str_canalizados = ""
    chunc_counter = 0
    for key in data.keys():
        # insert in chunks of 10
        if not sql_str_telemedidas:
            sql_str_telemedidas = f"insert into Telemedidas_SAP ({','.join(fields_in_Telemedidas)}) values "
            fields_telemedidas = tuple([data[key][field] for field in fields_for_Telemedidas])
        else:
            sql_str_telemedidas += ","
            fields_telemedidas += tuple([data[key][field] for field in fields_for_Telemedidas])
        sql_str_telemedidas += f"({','.join(['%s'] * len(fields_for_Telemedidas))})"
        if not sql_str_canalizados:
            sql_str_canalizados = f"insert ignore into Canalizados ({','.join(fields_in_Canalizados)}) values "
            fields_canalizados = tuple([data[key][field] for field in fields_for_Canalizados])
        else:
            sql_str_canalizados += ","
            fields_canalizados += tuple([data[key][field] for field in fields_for_Canalizados])
        sql_str_canalizados += f"({','.join(['%s'] * len(fields_for_Canalizados))})"
        if chunc_counter == 9:
            cur.execute(sql_str_telemedidas, fields_telemedidas)
            sql_str_telemedidas = ""
            chunc_counter = 0
            cur.execute(sql_str_canalizados, fields_canalizados)
            sql_str_canalizados = ""
    if sql_str_canalizados:
        cur.execute(sql_str_canalizados, fields_canalizados)
    if sql_str_telemedidas:
        cur.execute(sql_str_telemedidas, fields_telemedidas)
        
    # con.rollback() 
    con.commit()
        

    print(data)

# Validations for all Canalizado in CanalizadoAux with proveedor = '0'
#
# 1. Check that Telefono and CCanalizado in Lecturas is the same than in Telemedidas_SAP Telefono and CCanalizado

def validations():
    select_aux = "select CCanalizado,   from CanalizadoAux where Proveedor = '0'"
    select_Telemedidas_SAP = "select CCanalizado, Telefono from Telemedidas_SAP where CCanalizado in (select CCanalizado from CanalizadoAux where Proveedor = '0')"
    select_Telemedidas = "select CCanalizado, Telefono from Telemedidas where CCanalizado in (select CCanalizado from CanalizadoAux where Proveedor = '0')"
    composed = "select ts.CCanalizado, ts.Telefono, ts.NSerial, t.CCanalizado, t.telefono, t.NSerial from Telemedidas_SAP ts inner join Telemedidas t on ts.CCanalizado = t.CCanalizado where ts.CCanalizado in (select CCanalizado from DepositosAux where Proveedor = '0') and (t.NSerial != ts.NSerial or ts.Telefono != t.Telefono)"
  


if __name__ == "__main__":
    #main()
    validations()
