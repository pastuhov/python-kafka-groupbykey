import json
from confluent_kafka.serialization import Deserializer, SerializationError
from confluent_kafka.serialization import Serializer


class DummyDeserializer(Deserializer):
    def __call__(self, value, ctx=None):
        if value is None:
            return None

        return ''


class JSONDeserializer(Deserializer):
    """JSONDeserializer class"""
    def __call__(self, value, ctx=None):
        if value is None:
            return None
        try:
            return json.loads(value)
        except Exception as exception:
            raise SerializationError from exception


class JSONSerializer(Serializer):
    """JSONSerializer class"""
    def __call__(self, value, ctx=None):
        if value is None:
            return None
        try:
            return json.dumps(value)
        except Exception as exception:
            raise SerializationError from exception
