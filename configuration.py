from serde import JSONDeserializer, JSONSerializer, DummyDeserializer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.serialization import StringSerializer
from dataclasses import dataclass, field


@dataclass
class Configuration:

    commit_log_topic: str
    snapshot_topic: str
    bootstrap_servers: str
    group_id: str
    batch_timeout_sec: int = 5
    messages_per_transaction: int = 2000

    store_consumer: dict = field(default_factory=lambda: {
        'bootstrap.servers': None,
        'group.id': None,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'enable.auto.offset.store': False,
        'enable.partition.eof': True,
        'key.deserializer': StringDeserializer(),
        'value.deserializer': JSONDeserializer(),
        # 'stats_cb': publish_statistics,
        # 'statistics.interval.ms': 15000,
    })

    consumer: dict = field(default_factory=lambda: {
        'bootstrap.servers': None,
        'group.id': None,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'enable.auto.offset.store': False,
        'enable.partition.eof': False,
        'key.deserializer': StringDeserializer(),
        'value.deserializer': JSONDeserializer(),
        # 'value.deserializer': DummyDeserializer(),
        # 'stats_cb': publish_statistics,
        # 'statistics.interval.ms': 15000,
    })

    producer: dict = field(default_factory=lambda: {
        'bootstrap.servers': None,
        'transactional.id': None,
        'transaction.timeout.ms': 60000,
        'enable.idempotence': True,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': JSONSerializer(),
        'debug': 'broker,eos',
    })

    def __post_init__(self):
        self.store_consumer['bootstrap.servers'] = \
            self.consumer['bootstrap.servers'] = \
            self.producer['bootstrap.servers'] = \
            self.bootstrap_servers

        self.store_consumer['group.id'] = \
            self.consumer['group.id'] = \
            self.producer['transactional.id'] = \
            self.group_id
