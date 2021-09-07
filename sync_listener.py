from datetime import datetime
from download_file import dowload_strings, bucket_name
from google.cloud import pubsub_v1  
from time import sleep
from cep_credentials import gce_cert_json_name, cic_host, cic_user, cic_pass, cic_database
import MySQLdb as mdb

subscription_name = 'projects/mquinteiro/topics/cepsa_sync'

# Connect to mysql, start transaction, execute the querys in the string, commit and close connection
def exec_query(queries):
    db = mdb.connect(host=cic_host, user=cic_user, passwd=cic_pass, db=cic_database)
    cur = db.cursor()
    cur.auto_commit = False
    for query in queries:
        if query:
            cur.execute(query)
    db.commit()

# Instantiates a client and wait for pubsub events
def main():

    with pubsub_v1.SubscriberClient.from_service_account_json(gce_cert_json_name) as subscriber:
        sub_path=subscriber.subscription_path(
            'mquinteiro', 'cepsa_sync-sub')
        print('Listening for messages on {}'.format(sub_path))
        while True:
            response = subscriber.pull(subscription=sub_path, max_messages=1)
            if response:
                for msg in response.received_messages:
                    print("Received message for file:", msg.message.data)
                    strings = dowload_strings(bucket_name, msg.message.data)
                    # print(strings)
                    # todo dowload file form cloud and execute mysqldump with it.
                    if not strings:
                        subscriber.acknowledge(subscription=sub_path, ack_ids=[msg.ack_id])
                        print("Removing msg without file")
                    else:
                        print("Starting dump at:", datetime.now())
                        subscriber.modify_ack_deadline(subscription=sub_path, ack_ids=[msg.ack_id],ack_deadline_seconds=60)
                        querys = strings.replace(b'\r',b'').split(b'\n')
                        exec_query(querys)
                        subscriber.acknowledge(subscription=sub_path, ack_ids=[msg.ack_id])
                        print("Finishing dump at:", datetime.now())
                        print("Everithing is done, I'm going to sleep")
            sleep(1)

            




if __name__ == '__main__':
    main()
    
