# By MQA Generación de ficheros de traspaso a SAP desde la base de datos de la aplicación

# we will import date form sql server and generate a csv file
# the source table Lecturas and Telemedidas



from builtins import delattr
from cep_credentials import con_string, gce_cert_json_name
from time import sleep
from datetime import datetime, timedelta
import os

from cep_credentials import gce_cert_json_name, cic_host, cic_user, cic_pass, cic_database
import MySQLdb as mdb

def linux_connect():
    db = mdb.connect(host=cic_host, user=cic_user, passwd=cic_pass, db=cic_database)
    cur = db.cursor()
    cur.auto_commit = False
    return cur, db

def gen_lecturas_strings(last_date):
    cursor, cnxn = linux_connect()

    sql_str = f"select L.NSerial, T.Telefono, T.CCanalizado, DATE_FORMAT(HorLectura, '%d/%m/%Y')  dia "\
        f", DATE_FORMAT(HorLectura, '%H:%i')  hora, Nivel1, Nivel2, Nivel3" \
        f" from Lecturas L, Telemedidas T, DepositosAux D "\
        f" where L.NSerial=T.NSerial and L.NSerial = D.NSerial "\
        f" and HorLectura>'{last_date}' and D.Proveedor =0 "\
        f" order by L.NSerial";
    
    cursor.execute(sql_str)
    columns = '|'.join([column[0] for column in cursor.description])
    # result_strings = [columns]  # use this if you want to add the header
    result_strings = []
    for row in cursor.fetchall():
        query = "|".join([str(row[i] if row[i] else '0') for i in range(0, len(row))])
        result_strings.append(query)
    cnxn.close()
    return result_strings

def get_last_sync():
    cursor, cnxn = linux_connect()
    cursor.execute("select UltimaGrabacion from GrabacionLecturasTXT")
    return cursor.fetchone()[0]

def set_last_sync(last_sync=datetime.now()):
    cursor, cnxn = linux_connect()
    cursor.execute(f"update GrabacionLecturasTXT set UltimaGrabacion='{last_sync.strftime('%Y-%m-%d %H:%M:%S')}'")
    cnxn.commit()
    cnxn.close()

def main():
    last_sync = get_last_sync()
    last_date = datetime.strftime(last_sync, '%Y-%m-%d %H:%M:%S')
    file_name = f"Lecturas_{datetime.strftime(datetime.now(),'%H%M%d%m%Y')}_prg.txt"
    print(f"Last sync: {last_date}")
    end_date = datetime.now()
    end_date = datetime(2022,3,28,6,55)  # temporal para forzar desde ayer.
    result_strings = gen_lecturas_strings(last_date)
    print(f"{len(result_strings)} lines")
    if result_strings:
        with open(file_name, 'w') as f:
            for line in result_strings:
                f.write(line + '\n')
        set_last_sync(end_date)

if __name__ == '__main__':
    main()

