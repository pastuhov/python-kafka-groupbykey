import time


class Reducer:
    def __init__(
            self,
            reducer,
            consumer,
            producer,
            store,
            commit_log_topic,
            snapshot_topic,
            batch_timeout_sec=5,
            messages_per_transaction=2000):
        self.reducer = reducer
        self.consumer = consumer
        self.producer = producer
        self.store = store
        self.snapshot_topic = snapshot_topic
        self.commit_log_topic = commit_log_topic
        self.batch_timeout_sec = batch_timeout_sec
        self.messages_per_transaction = messages_per_transaction

    def process(self):
        self.producer.init_transactions()
        self.consumer.subscribe([self.commit_log_topic])

        timeout = time.time() + self.batch_timeout_sec
        messages = []
        while True:
            msg = self.consumer.poll(1)
            if msg is not None:
                messages.append(msg)
            if ((time.time() > timeout or len(messages) % self.messages_per_transaction == 0))\
                    and len(messages) > 0:
                self.producer.begin_transaction()
                # reduce
                for message in messages:
                    self.store.set(message.key(), self.reducer(self.store.get(message.key()), message))
                    self.producer.produce(
                        self.snapshot_topic,
                        value=self.store.get(message.key()).value,
                        key=message.key(),
                        headers=self.store.get(message.key()).headers,
                    )
                timeout = time.time() + self.batch_timeout_sec
                messages = []
                self.producer.send_offsets_to_transaction(self.consumer.position(
                    self.consumer.assignment()),
                    self.consumer.consumer_group_metadata())
                self.producer.commit_transaction()
