import os
import urllib.request
from pathlib import Path
from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    # =========================================================================
    # 1. ENVIRONMENT CONFIGURATION & JAR AUTOMATION
    # =========================================================================
    KAFKA_BROKER = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:29092")
    )
    
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)
    config = t_env.get_config().get_configuration()
    
    print("=====================================================================")
    print(f"🚀 Initializing PyFlink Engine [Target Broker: {KAFKA_BROKER}]")
    print("=====================================================================")

    BASE_DIR = Path(__file__).resolve().parent.parent
    LIBS_DIR = BASE_DIR / "libs"
    LIBS_DIR.mkdir(exist_ok=True)

    kafka_jar_url = "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar"
    json_jar_url = "https://repo1.maven.org/maven2/org/apache/flink/flink-json/1.18.0/flink-json-1.18.0.jar"

    kafka_jar_path = LIBS_DIR / "flink-sql-connector-kafka-3.0.1-1.18.jar"
    json_jar_path = LIBS_DIR / "flink-json-1.18.0.jar"

    print("[Flink Init] Checking local dependency JARs...")
    if not kafka_jar_path.exists():
        print(f"📥 Downloading Kafka Connector JAR to: {kafka_jar_path}")
        urllib.request.urlretrieve(kafka_jar_url, kafka_jar_path)

    if not json_jar_path.exists():
        print(f"📥 Downloading JSON Format JAR to: {json_jar_path}")
        urllib.request.urlretrieve(json_jar_url, json_jar_path)

    config.set_string(
        "pipeline.jars",
        f"{kafka_jar_path.as_uri()};{json_jar_path.as_uri()}"
    )
    
    config.set_string("table.exec.state.ttl", "24 h")
    config.set_string("table.exec.source.idle-timeout", "5000 ms")

    # =========================================================================
    # 2. SOURCE TABLE DEFINITIONS
    # =========================================================================
    print("[Flink Init] Mapping CDC Relational Streams with Business Clocks...")

    t_env.execute_sql(f"""
        CREATE TABLE src_login_events (
            after ROW<event_id STRING, customer_id STRING, success BOOLEAN, `timestamp` BIGINT>, op STRING,
            event_id AS after.event_id, customer_id AS after.customer_id, success AS after.success,
            event_time AS TO_TIMESTAMP_LTZ(after.`timestamp` / 1000, 3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH ('connector' = 'kafka', 'topic' = 'banking.login_events', 'properties.bootstrap.servers' = '{KAFKA_BROKER}', 'properties.group.id' = 'flink-cep', 'scan.startup.mode' = 'earliest-offset', 'format' = 'json')
    """)

    t_env.execute_sql(f"""
        CREATE TABLE src_beneficiaries (
            after ROW<beneficiary_id STRING, customer_id STRING, account_number STRING, created_at BIGINT>, op STRING,
            beneficiary_id AS after.beneficiary_id, customer_id AS after.customer_id, account_number AS after.account_number,
            event_time AS TO_TIMESTAMP_LTZ(after.created_at / 1000, 3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH ('connector' = 'kafka', 'topic' = 'banking.beneficiaries', 'properties.bootstrap.servers' = '{KAFKA_BROKER}', 'properties.group.id' = 'flink-cep', 'scan.startup.mode' = 'earliest-offset', 'format' = 'json')
    """)

    t_env.execute_sql(f"""
        CREATE TABLE src_transactions (
            after ROW<transaction_id STRING, from_account STRING, to_account STRING, amount DOUBLE, `timestamp` BIGINT>, op STRING,
            transaction_id AS after.transaction_id, from_account AS after.from_account, to_account AS after.to_account, amount AS after.amount,
            event_time AS TO_TIMESTAMP_LTZ(after.`timestamp` / 1000, 3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH ('connector' = 'kafka', 'topic' = 'banking.transactions', 'properties.bootstrap.servers' = '{KAFKA_BROKER}', 'properties.group.id' = 'flink-cep', 'scan.startup.mode' = 'earliest-offset', 'format' = 'json')
    """)

    t_env.execute_sql(f"""
        CREATE TABLE src_accounts (
            after ROW<account_id STRING, customer_id STRING, balance DOUBLE, currency STRING, status STRING, created_at BIGINT>, op STRING,
            account_id AS after.account_id, customer_id AS after.customer_id,
            event_time AS TO_TIMESTAMP_LTZ(after.created_at / 1000, 3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH ('connector' = 'kafka', 'topic' = 'banking.accounts', 'properties.bootstrap.servers' = '{KAFKA_BROKER}', 'properties.group.id' = 'flink-cep', 'scan.startup.mode' = 'earliest-offset', 'format' = 'json')
    """)

    # =========================================================================
    # 3. DATA STREAM UNIFICATION 
    # =========================================================================
    print("[Flink Processing] Unifying disparate streams into Timeline View...")
    t_env.execute_sql("""
        CREATE VIEW view_unified_timeline AS
        
        SELECT 
            customer_id, 
            'LOGIN_FAILED' AS event_type, 
            CAST(event_time AS TIMESTAMP_LTZ(3)) AS event_time, 
            CAST(0.0 AS DOUBLE) AS amount
        FROM src_login_events WHERE success = FALSE AND (op = 'c' OR op = 'r')
        
        UNION ALL
        
        SELECT 
            customer_id, 
            'BENEFICIARY_ADD' AS event_type, 
            CAST(event_time AS TIMESTAMP_LTZ(3)) AS event_time, 
            CAST(0.0 AS DOUBLE) AS amount
        FROM src_beneficiaries WHERE (op = 'c' OR op = 'r')
        
        UNION ALL
        
        SELECT 
            a.customer_id, 
            'TRANSFER' AS event_type, 
            CAST(t.event_time AS TIMESTAMP_LTZ(3)) AS event_time, 
            CAST(t.amount AS DOUBLE) AS amount
        FROM src_transactions t 
        JOIN src_accounts a ON t.from_account = a.account_id 
        WHERE (t.op = 'c' OR t.op = 'r') AND (a.op = 'c' OR a.op = 'r')
    """)

    print("[Flink Infrastructure] Setting up intermediate derived topic...")
    t_env.execute_sql(f"""
        CREATE TABLE kafka_unified_timeline (
            customer_id STRING,
            event_type STRING,
            event_time TIMESTAMP_LTZ(3),
            amount DOUBLE,
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'banking.unified_timeline',
            'properties.bootstrap.servers' = '{KAFKA_BROKER}',
            'properties.group.id' = 'flink-cep-pattern-matcher',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)

    # =========================================================================
    # 4. SINK DEFINITIONS (NEW: Outputting directly to Risk Engine Topic)
    # =========================================================================
    # LIVE SINK: Pushes structured alerts to Kafka for Phase 6
    t_env.execute_sql(f"""
        CREATE TABLE kafka_fraud_alerts (
            customer_id STRING,
            risk_indicator_type STRING,
            initial_failed_login TIMESTAMP_LTZ(3),
            beneficiary_added_time TIMESTAMP_LTZ(3),
            exfiltration_transfer_time TIMESTAMP_LTZ(3),
            stolen_funds DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'fraud_alerts',
            'properties.bootstrap.servers' = '{KAFKA_BROKER}',
            'format' = 'json'
        )
    """)

    # Debug Sink: To see raw unified timeline in the console
    t_env.execute_sql("""
        CREATE TEMPORARY TABLE debug_raw_stream (
            customer_id STRING,
            event_type STRING,
            event_time TIMESTAMP_LTZ(3),
            amount DOUBLE
        ) WITH ('connector' = 'print')
    """)

    # =========================================================================
    # 5. STATEMENT SET EXECUTION
    # =========================================================================
    print("=====================================================================")
    print(" 🚨 DUAL RUNTIME ACTIVE: Routing Streams & Hunting for ATO Patterns... ")
    print(" (Press Ctrl+C to terminate the streaming cluster) ")
    print("=====================================================================")

    stmt_set = t_env.create_statement_set()

    # Task 1: Feed view into intermediate Kafka table
    stmt_set.add_insert_sql("""
        INSERT INTO kafka_unified_timeline
        SELECT * FROM view_unified_timeline
    """)

    # Task 2: MATCH_RECOGNIZE query -> Sink to Phase 6 Kafka Topic
    stmt_set.add_insert_sql("""
        INSERT INTO kafka_fraud_alerts
        SELECT 
            customer_id,
            'CEP_ATO_MATCH' AS risk_indicator_type,
            initial_failed_login,
            beneficiary_added_time,
            exfiltration_transfer_time,
            stolen_funds
        FROM kafka_unified_timeline
        MATCH_RECOGNIZE (
            PARTITION BY customer_id
            ORDER BY event_time
            MEASURES
                F.event_time AS initial_failed_login,
                B.event_time AS beneficiary_added_time,
                T.event_time AS exfiltration_transfer_time,
                T.amount AS stolen_funds
            ONE ROW PER MATCH
            AFTER MATCH SKIP PAST LAST ROW
            PATTERN (F Noise1*? B Noise2*? T) WITHIN INTERVAL '1' HOUR
            DEFINE
                F AS F.event_type = 'LOGIN_FAILED',
                B AS B.event_type = 'BENEFICIARY_ADD',
                T AS T.event_type = 'TRANSFER'
        )
    """)

    # Task 3: Print all raw events entering the pattern matcher (Debugging)
    stmt_set.add_insert_sql("""
        INSERT INTO debug_raw_stream
        SELECT * FROM kafka_unified_timeline
    """)

    stmt_set.execute().wait()

if __name__ == '__main__':
    main()