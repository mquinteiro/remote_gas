# By MQA Generación de ficheros de traspaso a SAP desde la base de datos de la aplicación

# we will import date form sql server and generate a csv file
# the source table Lecturas and Telemedidas



from cep_credentials import con_string, gce_cert_json_name
from time import sleep
from datetime import datetime
import pyodbc
import os


def cepsa_connect():
    pyodbc.drivers()
    cnxn = pyodbc.connect(con_string)
    sql_str = "SET DATEFORMAT ymd"
    cursor = cnxn.cursor()
    cursor.execute(sql_str)
    return cnxn

def gen_lecturas_strings(last_date):
    cnxn = cepsa_connect()
    cursor = cnxn.cursor()

    sql_str = f"select L.NSerial, T.Telefono, T.CCanalizado, FORMAT (HorLectura, 'dd/MM/yyyy') dia "\
        f", FORMAT (HorLectura, 'hh:mm') hora, Nivel1, Nivel2, Nivel3" \
        f" from ceghcb.Lecturas L, ceghcb.Telemedidas T where L.NSerial=T.NSerial "\
        f" and HorLectura>'{last_date}' order by L.NSerial";
    
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
    cnxn = cepsa_connect()
    cursor = cnxn.cursor()
    cursor.execute("select UltimaGrabacion from ceghcb.GrabacionLecturasTXT")
    return cursor.fetchone()[0]

def set_last_sync(last_sync=datetime.now()):
    cnxn = cepsa_connect()
    cursor = cnxn.cursor()
    cursor.execute(f"update ceghcb.GrabacionLecturasTXT set UltimaGrabacion='{last_sync.strftime('%Y-%m-%d %H:%M:%S')}'")
    cnxn.commit()
    cnxn.close()

def main():
    last_sync = get_last_sync()
    last_date = datetime.strftime(last_sync, '%Y-%m-%d %H:%M:%S')
    print(f"Last sync: {last_date}")
    result_strings = gen_lecturas_strings(last_date)
    print(f"{len(result_strings)} lines")
    if result_strings:
        with open('lecturas.txt', 'w') as f:
            for line in result_strings:
                f.write(line + '\n')
        # set_last_sync(last_date)

if __name__ == '__main__':
    main()

