from download_file import download_file, get_default_bucket, bucket_name
from cep_credentials import gce_cert_json_name
from google.cloud import storage
import datetime
import argparse


def main():
    parser = argparse.ArgumentParser(description='Downloads the files from one day from the GCS.')
    parser.add_argument(dest='prefix', help='The download prefix to download from de GCS',
                        type=str, default=datetime.datetime.now().strftime('%Y%m%d'))
    args = parser.parse_args()
    client = storage.Client.from_service_account_json(gce_cert_json_name)
    blobs = client.list_blobs(bucket_name, prefix=args.prefix)
    for blob in blobs:
        blob.download_to_filename(blob.name)


if __name__ == '__main__':
    main()

