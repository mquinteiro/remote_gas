from google.cloud import pubsub
ce_cert_json_name = 'gce_cert.json'

pubsub_client = pubsub.Client.from_service_account_json(gce_cert_json_name)
topic = pubsub_client.topic('cepsa_sync')
    
for i in range(0,10):
    print("Publishing message {}".format(i))
    publisher.publish(topic='my-topic', data=str(i))