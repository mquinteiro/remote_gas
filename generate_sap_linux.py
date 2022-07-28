# By MQA Generación de ficheros de traspaso a SAP desde la base de datos de la aplicación

# we will import date form sql server and generate a csv file
# the source table Lecturas and Telemedidas



from builtins import delattr
from distutils.file_util import move_file
import shutil
from cep_credentials import con_string, gce_cert_json_name
from cep_credentials import sftp_dev_password, sftp_dev_user, sftp_pro_password, sftp_pro_user, sftp_host
from time import sleep
from datetime import datetime, timedelta
import os
import pysftp
from cep_credentials import gce_cert_json_name, cic_host, cic_user, cic_pass, cic_database
import MySQLdb as mdb
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from os.path import basename
import smtplib

support_team = ['mquinteiro@cic-systems.com', 'jesus.martinez@cic-systems.com']

def sendEmailData(fromaddr, toaddrs, files,subj="", body=""):
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = ', '.join(toaddrs)
    msg['Date'] = formatdate(localtime=True)
    if(subj==""):
        msg['Subject'] = "Canalizados "
    else:
        msg['Subject'] = subj
    msg.attach(MIMEText(body, 'plain'))
    msg.attach(MIMEText("Canalizados "))
    for f in files or []:
        with open(f, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(f)
            )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
        msg.attach(part)
    server = smtplib.SMTP("192.168.65.41")
    # server = smtplib.SMTP("mail.cic-systems.com")
    # server.set_debuglevel(1)
    server.send_message(msg)
    server.quit()


def linux_connect():
    db = mdb.connect(host=cic_host, user=cic_user, passwd=cic_pass, db=cic_database)
    cur = db.cursor()
    cur.auto_commit = False
    return cur, db

def gen_lecturas_strings(last_date):
    cursor, cnxn = linux_connect()

    sql_str = f"select L.NSerial, T.Telefono, T.CCanalizado, DATE_FORMAT(HorLectura, '%d/%m/%Y')  dia "\
        f", DATE_FORMAT(HorLectura, '%H:%i')  hora, Nivel1, Nivel2, Nivel3" \
        f" from Lecturas L, Telemedidas_SAP T, DepositosAux D "\
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
    temp_file_name = f"Lecturas_{datetime.strftime(datetime.now(),'%H%M%d%m%Y')}_prg.tmp"
    print(f"Last sync: {last_date}")
    end_date = datetime.now()
    # end_date = datetime(2022,4,28,6,55)  # temporal para forzar desde ayer.
    result_strings = gen_lecturas_strings(last_date)
    print(f"{len(result_strings)} lines")
    success = False
    # try to upload to the sftp, if it fails, send email to the support team.
    try:
        if result_strings:
            with open(file_name, 'w') as f:
                for line in result_strings:
                    f.write(line + '\n')
            # save to sftp server at /dat dir with temp file name and then rename to final file name
            with pysftp.Connection(sftp_host, username=sftp_pro_user, password=sftp_pro_password) as sftp:
                sftp.cwd('dat')
                sftp.put(file_name, temp_file_name)
                sftp.rename(temp_file_name, file_name)
            # if nothing breaks, set last sync to now
            set_last_sync(end_date)
            success = True
            
    except Exception as e:
        print(e)
        # send email to the support team
        sendEmailData('mquinteiro@cic-systems.com', support_team, [file_name], subj="Error enviando GAISB", 
                        body=f"Error uploading to sftp:\n {e}")
    # temporal, send email to the support team
    if success:
        sendEmailData('mquinteiro@cic-systems.com', support_team, [file_name], subj="Envio Correcto SAP GASIB",
                    body="Envio Correcto, dejar de enviar esto en un tiempo, solo para comprobación")
        # if processed dir do not exists, create it
        if not os.path.exists('./processed'):
            os.makedirs('./processed')
        # move the file to the processed dir
        
        move_file(file_name,"./processed/")
    else:
        # if errors dir do not exists, create it
        if not os.path.exists('./errors'):
            os.makedirs('./errors')
        # move the file to the processed dir
        move_file(file_name,"./errors/")
    

if __name__ == '__main__':
    main()

