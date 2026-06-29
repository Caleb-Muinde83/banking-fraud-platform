import os
import json
import uuid
import time
from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
from pyflink.datastream.state import ValueStateDescriptor

# -----------------------------------------------------------------------------
# 1. RISK CONFIGURATION MATRIX
# -----------------------------------------------------------------------------
RISK_WEIGHTS = {
    "VELOCITY_VIOLATION": 30,
    "SUSPICIOUS_DEVICE": 40,
    "IMPOSSIBLE_TRAVEL": 50,
    "LARGE_TRANSFER_ANOMALY": 35
}
THRESHOLD = 80

# -----------------------------------------------------------------------------
# 2. STATEFUL RISK PROCESSOR
# -----------------------------------------------------------------------------
class RiskScoringProcessor(KeyedProcessFunction):
    def __init__(self):
        self.state_descriptor = None

    def open(self, context):
        # We store a dict inside Flink's managed state: {"score": int, "indicators": list}
        self.state_descriptor = ValueStateDescriptor("identity_risk_state", Types.STRING())

    def process_element(self, value_str, ctx, out):
        # Parse incoming alert string from Phase 1
        try:
            alert = json.loads(value_str)
        except Exception:
            return

        customer_id = ctx.get_current_key()
        indicator = alert.get("risk_indicator_type", "UNKNOWN")
        weight = RISK_WEIGHTS.get(indicator, 10)

        # Retrieve current Flink managed state
        state_backend = ctx.get_key_state(self.state_descriptor)
        current_state_str = state_backend.value()

        if current_state_str is None:
            current_state = {"score": 0, "indicators": []}
        else:
            current_state = json.loads(current_state_str)

        # Update running tallies
        current_state["score"] += weight
        current_state["indicators"].append(indicator)

        print(f"📊 [FLINK STATE] Customer: {customer_id} | Added: {indicator} (+{weight}) | New Cumulative Score: {current_state['score']}")

        # Evaluate the 80-point threshold breach
        if current_state["score"] >= THRESHOLD:
            critical_alert = {
                "alert_id": f"ALT-{uuid.uuid4().hex[:10].upper()}",
                "customer_id": customer_id,
                "cumulative_score": current_state["score"],
                "contributing_indicators": current_state["indicators"],
                "timestamp": int(time.time() * 1000),
                "action_required": "REQUIRE_MFA" if current_state["score"] < 100 else "LOCK_ACCOUNT"
            }
            
            print(f"🚨 [FLINK THRESHOLD BREACHED] Firing critical alert for {customer_id}!")
            
            # Emit to down-stream Kafka sink
            out.collect(json.dumps(critical_alert))
            
            # Reset Flink state for this specific key
            state_backend.clear()
        else:
            # Save updated state back to Flink memory
            state_backend.update(json.dumps(current_state))


# -----------------------------------------------------------------------------
# 3. STREAMING ORCHESTRATION PIPELINE
# -----------------------------------------------------------------------------
def run_flink_risk_engine():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Keeps logs readable for debugging

    # Dynamically inject the Kafka connector JAR file
    # Ensure this JAR matches your exact Flink version (e.g., flink-sql-connector-kafka-3.1.0-SNAPSHOT or stable version)
    kafka_jar = os.path.join(os.getcwd(), "jars", "flink-sql-connector-kafka-3.0.1-1.18.jar")
    if os.path.exists(kafka_jar):
        env.add_jars(f"file:///{kafka_jar}")
    else:
        print(f"⚠️ Warning: Kafka connector JAR not found at {kafka_jar}. Streaming might fail.")

    # Define Kafka Source (Ingests from Phase 1 output)
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")) \
        .set_topics("fraud_alerts") \
        .set_group_id("flink-risk-group") \
        .set_value_only_deserializer(Types.STRING()) \
        .build()

    # Define Kafka Sink (Outputs Critical Alerts)
    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("critical_alerts")
                .set_value_serialization_schema(Types.STRING())
                .build()
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    # Stream Topology Definition
    stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka_Alerts_Source")
    
    # Extract customer_id dynamically to route events to partitioned states
    def get_customer_key(value_str):
        try:
            return json.loads(value_str).get("customer_id", "unknown_ip")
        except:
            return "unknown_ip"

    processed_stream = stream \
        .key_by(get_customer_key, key_type=Types.STRING()) \
        .process(RiskScoringProcessor(), output_type=Types.STRING())

    # Send breached thresholds to critical_alerts topic
    processed_stream.sink_to(kafka_sink)

    print("🚀 Apache Flink Stateful Risk Engine deployed successfully. Monitoring stream...")
    env.execute("Flink-Fraud-Risk-Scoring-Engine")


if __name__ == "__main__":
    run_flink_risk_engine()
