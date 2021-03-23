import logging
from confluent_kafka import KafkaError
from confluent_kafka.error import ConsumeError
from reduced import Reduced


class Store:
    def __init__(self, consumer, snapshot_topic):
        self.consumer = consumer
        self.snapshot_topic = snapshot_topic
        self.store = {}

    def hydrate(self):
        self.consumer.subscribe([self.snapshot_topic])
        while True:
            try:
                msg = self.consumer.poll(1)
                if msg is None:
                    continue
                self.set(msg.key(), Reduced(msg.value(), msg.headers()))
            except KeyboardInterrupt:
                logging.info('KeyboardInterrupt')
                break
            except ConsumeError as e:
                if e.code == KafkaError._PARTITION_EOF:
                    logging.info('The consumer reaches the end of a partition')
                    break

                raise ConsumeError
        self.consumer.close()

    def get(self, key):
        return self.store.get(key, None)

    def set(self, key, value):
        self.store[key] = value

    def length(self):
        return len(self.store)

    def keys(self):
        return self.store.keys()
