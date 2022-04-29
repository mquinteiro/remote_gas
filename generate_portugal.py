import os
from cep_credentials import gce_cert_json_name, cic_host, cic_user, cic_pass, cic_database
import MySQLdb as mdb
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from os.path import basename
from datetime import datetime, timedelta
import pandas as pd

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

def connect():
    db = mdb.connect(host=cic_host, user=cic_user, passwd=cic_pass, db=cic_database)
    cur = db.cursor()
    cur.auto_commit = False
    return cur, db


agencias = [(101, ["geral@gaslight.pt"], "GASLIGHT"), (103,["primegas.lda@gmail.com"], "Primegas--Tavira"),
            (102,["ana@agrotex.es","vitor.cepsapt@agrotex.es","andre.cepsapt@agrotex.es"],"Agrotex -- Coimbra") ]
def main():
    cursor, cnxn = connect()
    start_date = datetime.now() - timedelta(days=7, hours=1)
    start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
    headers = ['Agencia', "Codigo", "Cliente", "telefono", "Num_Serie", "Fecha_Ultima_Lectura", "Nivel", "temperatura" ]

    for cod_agencia, email, nombre in agencias:
        body = ";".join(headers) + "\n"
        sql = f"select '{nombre}', Codigo, Direccion, Telefono, CodEquipo, HorUltCons, NivelS1, Temperatura from Terminales where cliente = {cod_agencia} and HorUltCons > '{start_date_str}' order by CodEquipo"
        sql = f"select '{nombre}',  Codigo, Direccion, t.Telefono, t.CodEquipo, if(t.HorUltCons >= l.HorLectura or(l.HorLectura is null),HorUltCons ,l.HorLectura  ), "\
                f"if(t.HorUltCons >= l.HorLectura or(l.HorLectura is null),NivelS1 ,l.Nivel1  ) Nivel,"\
                f"  t.Temperatura from Terminales t left join Lecturas l on l.NSerial =t.CodEquipo  where cliente = {cod_agencia} "\
                f" and (l.HorLectura > '{start_date_str}' or HorUltCons > '{start_date_str}') order by CodEquipo"
        cursor.execute(sql)
        df = pd.DataFrame(cursor.fetchall(), columns=headers)
        #for row in cursor.fetchall():
        #    body += ",".join(str(x) for x in row) + "\n"
        # save body as file with name agencia_fecha_hora.csv
        filename = f"{cod_agencia}_{start_date.strftime('%Y-%m-%d_%H-%M-%S')}.xlsx"
        df.to_excel(filename, index=False)
        #with open(filename, "w") as f:
        #    f.write(body)
        # sendEmailData("Niveles Telemedidas CIC", email, [filename], body="Información semanal de los niveles de las telemedidas")
        # move the file to ./enviados_{doc_agencia} directory
        os.rename(filename, f"./enviados_{cod_agencia}/{filename}")
    cnxn.close()
    
if __name__ == "__main__":
    main()
