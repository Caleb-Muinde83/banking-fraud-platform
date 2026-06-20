import os
from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    # =========================================================================
    # 1. ENVIRONMENT CONFIGURATION
    # =========================================================================
    # Toggle this depending on where you are executing this script:
    # - Use 'localhost:9092' when running locally inside your host terminal .venv
    # - Use 'kafka:29092' when submitting this script directly inside the Docker cluster
    KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")
    
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)
    
    config = t_env.get_config().get_configuration()
    
    print("=====================================================================")
    print(f"🚀 Initializing PyFlink Engine [Target Broker: {KAFKA_BROKER}]")
    print("=====================================================================")

    # Inject external Java dependencies at runtime to process Kafka streams & Debezium JSON
    print("[Flink Init] Downloading Kafka and Debezium JSON Format JARs...")
    config.set_string(
        "pipeline.jars",
        "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar;"
        "https://repo1.maven.org/maven2/org/apache/flink/flink-json/1.18.0/flink-json-1.18.0.jar"
    )
    
    # Optimize Flink's managed state time-to-live (TTL) to clear out stale tracking data
    config.set("table.exec.state.ttl", "15s")

    # =========================================================================
    # 2. SOURCE TABLE DEFINITIONS (Consuming Debezium CDC Streams)
    # =========================================================================
    print("[Flink Init] Mapping Debezium CDC Relational Streams...")

    # Table A: Login Events (Captured via CDC or App-Telemetry)
    t_env.execute_sql(f"""
        CREATE TABLE src_login_events (
            event_id STRING,
            customer_id STRING,
            success BOOLEAN,
            `timestamp` TIMESTAMP(3),
            WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'bank.public.login_events',
            'properties.bootstrap.servers' = '{KAFKA_BROKER}',
            'properties.group.id' = 'flink-cep-logins',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'debezium-json'
        )
    """)

    # Table B: Beneficiary Allocations
    t_env.execute_sql(f"""
        CREATE TABLE src_beneficiaries (
            beneficiary_id STRING,
            customer_id STRING,
            account_number STRING,
            created_at TIMESTAMP(3),
            WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'bank.public.beneficiaries',
            'properties.bootstrap.servers' = '{KAFKA_BROKER}',
            'properties.group.id' = 'flink-cep-beneficiaries',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'debezium-json'
        )
    """)

    # Table C: Financial Ledger Transactions
    t_env.execute_sql(f"""
        CREATE TABLE src_transactions (
            transaction_id STRING,
            from_account STRING,
            to_account STRING,
            amount DOUBLE,
            `timestamp` TIMESTAMP(3),
            WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'bank.public.transactions',
            'properties.bootstrap.servers' = '{KAFKA_BROKER}',
            'properties.group.id' = 'flink-cep-transactions',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'debezium-json'
        )
    """)

    # Table D: Accounts (Identity Mapping Table)
    t_env.execute_sql(f"""
        CREATE TABLE src_accounts (
            account_id STRING,
            customer_id STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'bank.public.accounts',
            'properties.bootstrap.servers' = '{KAFKA_BROKER}',
            'properties.group.id' = 'flink-cep-accounts',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'debezium-json'
        )
    """)

    # =========================================================================
    # 3. DATA STREAM UNIFICATION & IDENTITY RESOLUTION
    # =========================================================================
    print("[Flink Processing] Unifying disparate streams into Timeline View...")
    t_env.execute_sql("""
        CREATE VIEW unified_customer_timeline AS
        
        -- Event Type 1: Failed Authentication
        SELECT customer_id, 'LOGIN_FAILED' AS event_type, `timestamp` AS event_time, 0.0 AS amount
        FROM src_login_events WHERE success = FALSE
        
        UNION ALL
        
        -- Event Type 2: High-Risk Infrastructure Mutation
        SELECT customer_id, 'BENEFICIARY_ADD' AS event_type, created_at AS event_time, 0.0 AS amount
        FROM src_beneficiaries
        
        UNION ALL
        
        -- Event Type 3: Outbound Transfer (Enriched via streaming identity lookup)
        SELECT a.customer_id, 'TRANSFER' AS event_type, t.`timestamp` AS event_time, CAST(t.amount AS DOUBLE) AS amount
        FROM src_transactions t
        INNER JOIN src_accounts a ON t.from_account = a.account_id
    """)

    # =========================================================================
    # 4. SINK DEFINITION (Where alerts are routed)
    # =========================================================================
    print("[Flink Sinks] Registering Downstream Fraud Alert Topic...")
    t_env.execute_sql(f"""
        CREATE TABLE sink_fraud_alerts (
            customer_id STRING,
            initial_failed_login TIMESTAMP(3),
            beneficiary_added_time TIMESTAMP(3),
            exfiltration_transfer_time TIMESTAMP(3),
            stolen_funds DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'fraud_alerts',
            'properties.bootstrap.servers' = '{KAFKA_BROKER}',
            'format' = 'json'
        )
    """)

    # =========================================================================
    # 5. COMPLEX EVENT PROCESSING (CEP) MATCH RECOGNITION
    # =========================================================================
    print("⚡ [CEP Engine] Arming Attack Signatures via MATCH_RECOGNIZE...")
    
    cep_fraud_query = """
        INSERT INTO sink_fraud_alerts
        SELECT customer_id, initial_failed_login, beneficiary_added_time, exfiltration_transfer_time, stolen_funds
        FROM unified_customer_timeline
        MATCH_RECOGNIZE (
            PARTITION BY customer_id
            ORDER BY event_time
            MEASURES
                FIRST(F.event_time) AS initial_failed_login,
                B.event_time AS beneficiary_added_time,
                T.event_time AS exfiltration_transfer_time,
                T.amount AS stolen_funds
            ONE ROW PER MATCH
            AFTER MATCH SKIP PAST LAST ROW
            PATTERN (F+ B T) WITHIN INTERVAL '15' MINUTE
            DEFINE
                F AS F.event_type = 'LOGIN_FAILED',
                B AS B.event_type = 'BENEFICIARY_ADD',
                T AS T.event_type = 'TRANSFER' AND T.amount >= 1000.0
        )
    """

    # Execute the persistent streaming query
    t_env.execute_sql(cep_fraud_query)
    print("=====================================================================")
    print(" CEP ENGINE RUNNING: Live scanning for multi-step ATO exploits...")
    print("=====================================================================")

if __name__ == '__main__':
    main()