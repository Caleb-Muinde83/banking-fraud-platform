import sys
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

aiokafka_stub = types.ModuleType("aiokafka")
class _AIOKafkaProducer:  # pragma: no cover - test stub
    def __init__(self, *args, **kwargs):
        pass

aiokafka_stub.AIOKafkaProducer = _AIOKafkaProducer
sys.modules.setdefault("aiokafka", aiokafka_stub)

confluent_kafka_stub = types.ModuleType("confluent_kafka")
class _Producer:  # pragma: no cover - test stub
    def __init__(self, *args, **kwargs):
        pass

class _SchemaRegistryClient:  # pragma: no cover - test stub
    def __init__(self, *args, **kwargs):
        pass

class _AvroSerializer:  # pragma: no cover - test stub
    def __init__(self, *args, **kwargs):
        pass

class _StringSerializer:  # pragma: no cover - test stub
    def __init__(self, *args, **kwargs):
        pass

class _SerializationContext:  # pragma: no cover - test stub
    def __init__(self, *args, **kwargs):
        pass

class _MessageField:  # pragma: no cover - test stub
    VALUE = "value"

confluent_kafka_stub.Producer = _Producer
sys.modules.setdefault("confluent_kafka", confluent_kafka_stub)

schema_registry_stub = types.ModuleType("confluent_kafka.schema_registry")
schema_registry_stub.SchemaRegistryClient = _SchemaRegistryClient
sys.modules.setdefault("confluent_kafka.schema_registry", schema_registry_stub)

avro_stub = types.ModuleType("confluent_kafka.schema_registry.avro")
avro_stub.AvroSerializer = _AvroSerializer
sys.modules.setdefault("confluent_kafka.schema_registry.avro", avro_stub)

serialization_stub = types.ModuleType("confluent_kafka.serialization")
serialization_stub.StringSerializer = _StringSerializer
serialization_stub.SerializationContext = _SerializationContext
serialization_stub.MessageField = _MessageField
sys.modules.setdefault("confluent_kafka.serialization", serialization_stub)

from app.main import build_provisional_customer


class CustomerProvisioningTests(unittest.TestCase):
    def test_build_provisional_customer_populates_required_fields(self):
        customer = build_provisional_customer("new-user", "KE")

        self.assertEqual(customer.customer_id, "new-user")
        self.assertEqual(customer.first_name, "Auto")
        self.assertEqual(customer.last_name, "Provisioned")
        self.assertEqual(customer.email, "new-user@auto-generated.io")
        self.assertEqual(customer.country, "KE")
        self.assertEqual(customer.risk_level, "LOW")


if __name__ == "__main__":
    unittest.main()
