from sys import stdout
from google.cloud import pubsub
from time import sleep
from cep_credentials import con_string, gce_cert_json_name

#ce_cert_json_name = 'gce_cert.json'
topic = 'projects/mquinteiro/topics/test_topic'
publisher = pubsub.PublisherClient.from_service_account_json(gce_cert_json_name)
    
with pubsub.SubscriberClient.from_service_account_json(gce_cert_json_name) as subscriber:
    subscription_path1 = subscriber.subscription_path('mquinteiro', 'test_topic-sub')
    subscription_path2 = subscriber.subscription_path('mquinteiro', 'test_topic-sub2')
    def callback(message):
        print('Received S1 message: {}'.format(message))
        stdout.flush()
        message.ack()
    def callback2(message):
        print('Received S2 message: {}'.format(message))
        stdout.flush()
        message.ack()
    # result = subscriber.subscribe(subscription_path1, callback=callback).result()
    subscriber.subscribe(subscription_path1, callback=callback)
    result = subscriber.subscribe(subscription_path2, callback=callback2)
    print(result)
    while True:
        sleep(3)   ## Consuming messages via callback.
        print(".", end="")
