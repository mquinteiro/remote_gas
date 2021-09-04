from google.cloud import storage
from sys import argv

if len(argv) <2:
    print("upload_file.py filename")
    exit(0)

print(argv[0])
file_name = argv[1]
storage_client = storage.Client.from_service_account_json('mquinteiro-8ef43ab73aee.json')
bucket_name = 'cepsa_shares'


bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(file_name)
blob.download_to_filename(file_name)
#blob.upload_from_filename(file_name)
