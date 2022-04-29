import MySQLdb as mdb
from cep_credentials import gce_cert_json_name, cic_host, cic_user, cic_pass, cic_database
import re
from download_file import dowload_strings, bucket_name, move_file

file_name = "processed/20220429190006_cepsa_update.txt"
strings = dowload_strings(bucket_name, file_name).decode('ISO-8859-1')
s = strings.replace('delete from Terminales;\r\r\n','delete from Terminales@ENDCOMAND@')
s2 = re.sub('\);\r+\n*',')@ENDCOMAND@',s)
queries = s2.split('@ENDCOMAND@')
for my_strings in queries:
    if my_strings.find("Centro Paroquial e Social") != -1:
        print(my_strings)
        db = mdb.connect(host=cic_host, user=cic_user, passwd=cic_pass, db=cic_database)
        cur = db.cursor()
        cur.execute(my_strings)
        db.commit()
        break

