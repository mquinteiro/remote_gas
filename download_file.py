from google.cloud import storage
from cep_credentials import gce_cert_json_name
from sys import argv

bucket_name = 'cepsa_shares'

def dowload_strings(bucket_name, blob_name):
    storage_client = storage.Client.from_service_account_json(gce_cert_json_name)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    if blob.exists():
        return blob.download_as_bytes()
        
    return None

def download_file(bucket_name, blob_name):
    storage_client = storage.Client.from_service_account_json(gce_cert_json_name)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.download_to_filename(file_name)


def move_to_processed(bucket_name, blob_name):
    storage_client = storage.Client.from_service_account_json(gce_cert_json_name)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    bucket.rename_blob(blob, b'processed/' + blob_name)

def move_to_failed(bucket_name, blob_name):
    storage_client = storage.Client.from_service_account_json(gce_cert_json_name)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    bucket.rename_blob(blob, b'failed/' + blob_name)

if __name__ == '__main__':    
    if len(argv) <2:
        print("upload_file.py filename")
        exit(0)
    print(argv[0])
    file_name = argv[1]





#blob.upload_from_filename(file_name)
