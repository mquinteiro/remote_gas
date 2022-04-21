from google.cloud import pubsub
from time import sleep
from cep_credentials import con_string, gce_cert_json_name

#ce_cert_json_name = 'gce_cert.json'
topic = 'projects/mquinteiro/topics/test_topic'
publisher = pubsub.PublisherClient.from_service_account_json(gce_cert_json_name)
    
for i in range(0,10000):
    print("Publishing message {}".format(i))
    result = publisher.publish(topic=topic, data=str(i).encode('utf-8')).result()
    # print(result)
    sleep(2)