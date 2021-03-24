# python-kafka-groupbykey

Realizes Kafka streams code: 
```java
builder.stream("commit_log_topic").groupByKey().aggregate(...).toStream().to("snapshot_topic");
```

## Example

```python
from confluent_kafka import SerializingProducer, DeserializingConsumer
from http_server import start_server
from reducer import Reducer
from store import Store
from reduced import Reduced
from configuration import Configuration
import sys

config = Configuration(
    commit_log_topic='commit_log_topic_name',
    snapshot_topic='snapshot_topic_topic_name',
    bootstrap_servers='localhost',
    group_id='app_name',
)

def reduce(accumulator, current):
    if accumulator is None:
        accumulator = Reduced()
        accumulator.value = {}
    
    # your reduce code

    return accumulator

s = Store(DeserializingConsumer(config.store_consumer), config.snapshot_topic)
s.hydrate()

start_server(s, 8333)

r = Reducer(
    reduce,
    DeserializingConsumer(config.consumer),
    SerializingProducer(config.producer),
    s,
    config.commit_log_topic,
    config.snapshot_topic,
    config.batch_timeout_sec,
    config.messages_per_transaction)
r.process()
```
