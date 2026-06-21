import os
import urllib.request
from pathlib import Path
from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    # =========================================================================
    # 1. ENVIRONMENT CONFIGURATION & JAR AUTOMATION
    # =========================================================================
    KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:29092")
    
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)
    
    config = t_env.get_config().get_configuration()
    
    print("=====================================================================")
    print(f"🚀 Initializing PyFlink Engine [Target Broker: {KAFKA_BROKER}]")
    print("=====================================================================")

    # Setup local directory to cache JAR files to bypass Flink's lack of HTTPS support
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

    # Pass local file:/// URIs directly to Flink configuration
    config.set_string(
        "pipeline.jars",
        f"{kafka_jar_path.as_uri()};{json_jar_path.as_uri()}"
    )
    
    config.set_string("table.exec.state.ttl", "15s")

    # =========================================================================
    # 2. SOURCE TABLE DEFINITIONS (Aligned with your exact Kafka topics)
    # =========================================================================
    print("[Flink Init] Mapping Debezium CDC Relational Streams...")

    # Login Events
    t_env.execute_sql(f"""
        CREATE TABLE src_login_events (
            after ROW<
                event_id STRING,
                customer_id STRING,
                success BOOLEAN,
                `timestamp` STRING
            >,
            op STRING,

            event_id AS after.event_id,
            customer_id AS after.customer_id,
            success AS after.success,
            `timestamp` AS CAST(after.`timestamp` AS TIMESTAMP(3)),

            WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'banking.login_events',
            'properties.bootstrap.servers' = '{KAFKA_BROKER}',
            'properties.group.id' = 'flink-cep-logins',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)

    # Beneficiaries
    t_env.execute_sql(f"""
        CREATE TABLE src_beneficiaries (
            after ROW<
                beneficiary_id STRING,
                customer_id STRING,
                account_number STRING,
                created_at STRING
            >,
            op STRING,

            beneficiary_id AS after.beneficiary_id,
            customer_id AS after.customer_id,
            account_number AS after.account_number,
            created_at AS CAST(after.created_at AS TIMESTAMP(3)),

            WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'banking.beneficiaries',
            'properties.bootstrap.servers' = '{KAFKA_BROKER}',
            'properties.group.id' = 'flink-cep-beneficiaries',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)

    # Transactions
    t_env.execute_sql(f"""
        CREATE TABLE src_transactions (
            after ROW<
                transaction_id STRING,
                from_account STRING,
                to_account STRING,
                amount DOUBLE,
                `timestamp` STRING
            >,
            op STRING,

            transaction_id AS after.transaction_id,
            from_account AS after.from_account,
            to_account AS after.to_account,
            amount AS after.amount,
            `timestamp` AS CAST(after.`timestamp` AS TIMESTAMP(3)),

            WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'banking.transactions',
            'properties.bootstrap.servers' = '{KAFKA_BROKER}',
            'properties.group.id' = 'flink-cep-transactions',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)

    # Accounts Table
    t_env.execute_sql(f"""
        CREATE TABLE src_accounts (
            account_id STRING,
            customer_id STRING,
            kafka_time TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
            PRIMARY KEY (account_id) NOT ENFORCED,
            WATERMARK FOR kafka_time AS kafka_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'banking.accounts',
            'properties.bootstrap.servers' = '{KAFKA_BROKER}',
            'properties.group.id' = 'flink-cep-accounts',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'debezium-json'
        )
    """)

    # =========================================================================
    # 3. DATA STREAM UNIFICATION (Using Event-Time Temporal Table Join)
    # =========================================================================
    print("[Flink Processing] Unifying disparate streams into Timeline View...")
    t_env.execute_sql("""
        CREATE VIEW unified_customer_timeline AS
        
        SELECT customer_id, 'LOGIN_FAILED' AS event_type, `timestamp` AS event_time, 0.0 AS amount
        FROM src_login_events 
        WHERE success = FALSE AND (op = 'c' OR op = 'r')
        
        UNION ALL
        
        SELECT customer_id, 'BENEFICIARY_ADD' AS event_type, created_at AS event_time, 0.0 AS amount
        FROM src_beneficiaries
        WHERE (op = 'c' OR op = 'r')
        
        UNION ALL
        
        SELECT a.customer_id, 'TRANSFER' AS event_type, t.`timestamp` AS event_time, CAST(t.amount AS DOUBLE) AS amount
        FROM src_transactions t
        JOIN src_accounts FOR SYSTEM_TIME AS OF t.`timestamp` AS a 
        ON t.from_account = a.account_id
        WHERE (t.op = 'c' OR t.op = 'r')
    """)

    # =========================================================================
    # 4. SINK DEFINITION
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

    table_result = t_env.execute_sql(cep_fraud_query)
    
    print("=====================================================================")
    print(" 🚨 CEP ENGINE ACTIVE: Live scanning for multi-step ATO exploits... ")
    print(" (Press Ctrl+C to terminate the streaming cluster) ")
    print("=====================================================================")

    # Keep the Python process alive to monitor the asynchronous streaming job execution
    job_client = table_result.get_job_client()
    if job_client is not None:
        job_client.get_job_execution_result().result()

if __name__ == '__main__':
    main()