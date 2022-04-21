from download_file import dowload_strings, bucket_name, move_file
from cep_credentials import gce_cert_json_name
from sync_listener import exec_query
from google.cloud import storage
import re
import datetime

def sync_file(file, bucket_name):
    strings = dowload_strings(bucket_name, file).decode('ISO-8859-1')
    s = strings.replace('delete from Terminales;\r\r\n','delete from Terminales@ENDCOMAND@')
    s2 = re.sub('\);\r+\n*',')@ENDCOMAND@',s)
    querys = s2.split('@ENDCOMAND@')
    for w in querys:
        if w[:7] != 'replace':
            print(w)
    exec_query(querys)


if __name__ == '__main__':
    client = storage.Client.from_service_account_json(gce_cert_json_name)

    bucket_name = 'cepsa_shares'
    names = []
    for blob in client.list_blobs(bucket_name, prefix='error/2022'):
        names.append(blob.name)
    names.sort()
    for name in names:
        print(name)
        # continue
        # sync_file(name, bucket_name)
        try:
            strings = dowload_strings(bucket_name, name).decode('ISO-8859-1')
            exec_query(strings)
            print("Finishing dump at:", datetime.datetime.now())
            move_file(bucket_name, name,  "processed/"+name)
        except Exception as e:
            print(e)
            move_file(bucket_name, name,  "errors/"+name)
