from cep_credentials import gce_cert_json_name, cic_host, cic_user, cic_pass, cic_database
import MySQLdb as mdb

from generate_sap_linux import sendEmailData


support_team = ['jesus.martinez@cic-systems.com']
#support_team = ['mquinteiro@gmail.com']

def linux_connect():
    db = mdb.connect(host=cic_host, user=cic_user, passwd=cic_pass, db='TGL_COM')
    cur = db.cursor()
    cur.auto_commit = False
    return cur, db


def main():
    cursor, cnxn = linux_connect()
    # get last id from table form two days ago in mysql
    cursor.execute("SELECT MAX(id) FROM TGL_COM.mensajeEntrante where `time` < DATE_SUB(NOW(), INTERVAL 2 DAY)")
    last_id = cursor.fetchone()[0]
    cursor.execute("SELECT DISTINCT(`from`) from mensajeEntrante where processed is NULL")
    lecturas = cursor.fetchall()
    unprocessed = []
    for lectura in lecturas:
        unprocessed.append(lectura[0])
    
    if len(unprocessed) > 0:
        body = f"{len(unprocessed)} Equipos no procesados"
        for unprocessed_equipo in unprocessed:
            body += f"\n{unprocessed_equipo}"
        sendEmailData('mquinteiro@cic-systems.com', support_team, None, subj='SMS no procesados', body=body)

        # if it is informed, move messages to mensajeEntrante_noProcesado older than 2 days

        cursor.execute(f"insert into mensajeEntrante_noProcesado select * from mensajeEntrante where processed is NULL and id <= {last_id}")
        # delete original messages
        cursor.execute(f"delete from mensajeEntrante where processed is NULL and id <= {last_id}")
        cnxn.commit()

    cursor.close()
    cnxn.close()

if __name__ == '__main__':
    main()
