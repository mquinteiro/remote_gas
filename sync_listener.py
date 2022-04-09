from datetime import datetime
from download_file import dowload_strings, bucket_name, move_file
from google.cloud import pubsub_v1  
from time import sleep
from cep_credentials import gce_cert_json_name, cic_host, cic_user, cic_pass, cic_database
import MySQLdb as mdb
import re
subscription_name = 'projects/mquinteiro/topics/cepsa_sync'


# Connect to mysql, start transaction, execute the querys in the string, commit and close connection
def exec_query(strings):
    db = mdb.connect(host=cic_host, user=cic_user, passwd=cic_pass, db=cic_database)
    cur = db.cursor()
    #cur.execute("DELIMITER @ENDCOMAND@")
    cur.auto_commit = False
    s = strings.replace('delete from Terminales;\r\r\n','delete from Terminales@ENDCOMAND@')
    s2 = re.sub('\);\r+\n*',')@ENDCOMAND@',s)
    queries = s2.split('@ENDCOMAND@')
    for query in queries:
        if query:
            try:
                cur.execute(query)
            except Exception as e:
                print(e)
                print(f"Error in query: {query}")
                exit(-1)
    #cur.execute("DELIMITER ;")
    db.commit()


# Instantiates a client and wait for pubsub events
def main():

    with pubsub_v1.SubscriberClient.from_service_account_json(gce_cert_json_name) as subscriber:
        sub_path = subscriber.subscription_path(
            'mquinteiro', 'cepsa_sync-sub')
        print('Listening for messages on {}'.format(sub_path))
        while True:
            response = subscriber.pull(subscription=sub_path, max_messages=1)
            if response:
                names = []
                for msg in response.received_messages:
                    print("Received message for file:", msg.message.data)
                    names.append(msg.message.data)
                    try:
                        strings = dowload_strings(bucket_name, msg.message.data).decode('ISO-8859-1')
                        # print(strings)
                        # todo dowload file form cloud and execute mysqldump with it.
                        if not strings:
                            subscriber.acknowledge(subscription=sub_path, ack_ids=[msg.ack_id])
                            print("Removing msg without file")
                            move_file(bucket_name, msg.message.data, b"empty/" + msg.message.data)
                            continue
                        else:
                            print("Starting dump at:", datetime.now())
                            subscriber.modify_ack_deadline(subscription=sub_path, ack_ids=[msg.ack_id],ack_deadline_seconds=60)
                            exec_query(strings)
                            subscriber.acknowledge(subscription=sub_path, ack_ids=[msg.ack_id])
                            move_file(bucket_name, msg.message.data, b"processed/" + msg.message.data)
                            print("Finishing dump at:", datetime.now())

                    except Exception as e:
                        print(e)
                        subscriber.acknowledge(subscription=sub_path, ack_ids=[msg.ack_id])
                        print(f"Removing msg with error: failed to process {msg.message.data}")
                        try:
                            move_file(bucket_name, msg.message.data, b"error/"+msg.message.data)
                        except Exception as e:
                            print(e)
                            print(f"Failed to move error {msg.message.data} file to error folder")
                print("Everithing is done, I'm going to sleep")
            sleep(1)


if __name__ == '__main__':
    main()
    
