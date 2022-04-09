from datetime import datetime
from download_file import dowload_strings, bucket_name, move_file
from google.cloud import pubsub_v1
from time import sleep
from cep_credentials import gce_cert_json_name, cic_host, cic_user, cic_pass, cic_database
import MySQLdb as mdb

from google.cloud import storage

from sync_listener import exec_query

if __name__ == '__main__':
    client = storage.Client.from_service_account_json(gce_cert_json_name)

    bucket_name = 'cepsa_shares'
    names = []
    for blob in client.list_blobs(bucket_name, prefix='2022'):
        names.append(blob.name)
    names.sort()
    for name in names:
        print(name)
        # continue
        strings = dowload_strings(bucket_name, name)
        if strings:
            print("Starting dump at: ", datetime.now(), f'file: {name}')
            querys = strings.replace(b'\r', b'').split(b'\n')
            try:
                exec_query(querys)
                print("Finishing dump at:", datetime.now())
                move_file(bucket_name, name,  "processed/"+name)
            except Exception as e:
                move_file(bucket_name, name,  "errors/"+name)
        else:
            move_file(bucket_name, name,  "empty/"+name)
