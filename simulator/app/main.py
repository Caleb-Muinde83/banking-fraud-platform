import asyncio
import random
import sys
import httpx
from faker import Faker
from app.actors.customer import AsyncCustomerActor, AsyncEmployeeActor
from app.attacks.scenarios import ScenarioManager

fake = Faker()

# Production Scale Variable Target
TARGET_NUM_CUSTOMERS = 20

async def customer_lifecycle_loop(actor: AsyncCustomerActor, client: httpx.AsyncClient):
    """Encapsulates a single user's lifecycle loop inside an independent async task context."""
    while True:
        try:
            await actor.perform_action(client)
            await asyncio.sleep(random.uniform(15.0, 90.0))  
        except Exception:
            await asyncio.sleep(10)

async def employee_lifecycle_loop(actor: AsyncEmployeeActor, client: httpx.AsyncClient, target_customer_ids):
    """Encapsulates a bank operator's administrative interaction tracking layout."""
    while True:
        try:
            await actor.perform_internal_action(client, target_customer_ids)
            await asyncio.sleep(random.uniform(30.0, 120.0))
        except Exception:
            await asyncio.sleep(15)

async def main_engine():
    print("==============================================")
    print("   LAUNCHING VIRTUAL USER BEHAVIOR ENGINE     ")
    print("==============================================")
    print(f"[Dynamic Provisioning] Generating {TARGET_NUM_CUSTOMERS} randomized user identity profiles...")
    
    customer_pool = []
    target_customer_ids = []
    
    for i in range(TARGET_NUM_CUSTOMERS):
        username_handle = f"cust_gen_{i}"
        persona = random.choice(["STUDENT", "BUSINESS", "PROFESSIONAL", "RETIREE"])
        country = "CA" if persona == "RETIREE" else "US"
        
        customer_pool.append(
            AsyncCustomerActor(
                username=username_handle,
                account_id=f"ACC-GEN{i:05d}",
                persona_type=persona,
                fixed_country=country
            )
        )
        target_customer_ids.append(username_handle)

    employee_pool = [
        AsyncEmployeeActor(employee_id="emp_teller_01", department="RETAIL_BANKING", role="TELLER"),
        AsyncEmployeeActor(employee_id="emp_mgr_02", department="COMPLIANCE", role="MANAGER")
    ]

    print(f"[Initialization] Configured {len(customer_pool)} dynamic user engines.")
    print(f"[Initialization] Configured {len(employee_pool)} internal bank staff threads.")
    print("[Initialization] Provisioning High-Throughput HTTP Client Resource Pools...")
    
    limits = httpx.Limits(max_keepalive_connections=300, max_connections=3000)
    
    async with httpx.AsyncClient(base_url="http://127.0.0.1:8000", limits=limits, timeout=10.0) as client:
        
        customer_tasks = [
            asyncio.create_task(customer_lifecycle_loop(cust, client)) 
            for cust in customer_pool
        ]
        employee_tasks = [
            asyncio.create_task(employee_lifecycle_loop(emp, client, target_customer_ids)) 
            for emp in employee_pool
        ]
        
        print(f"🚀 SCALED ENGINE RUNNING: {TARGET_NUM_CUSTOMERS} concurrent simulation nodes active.")
        print("Background pipeline execution running. Press CTRL+C to terminate.")
        
        # Orchestrate Threat Vectors on Asymmetric Global Clock Ticks
        loop_count = 0
        while True:
            loop_count += 1
            await asyncio.sleep(5)  # 5-second tick interval check
            
            # 1. Run Credential Stuffing campaigns frequently
            if loop_count % 4 == 0:
                asyncio.create_task(ScenarioManager.run_credential_stuffing())
                
            # 2. Run Password Spraying / MFA Fatigue campaigns
            if loop_count % 6 == 0:
                asyncio.create_task(ScenarioManager.run_auth_abuse_campaign(TARGET_NUM_CUSTOMERS))

            # 3. Run an Account Takeover against a generated target
            if loop_count % 8 == 0:
                rand_idx = random.randint(0, TARGET_NUM_CUSTOMERS - 1)
                asyncio.create_task(ScenarioManager.run_account_takeover(
                    victim_account=f"ACC-GEN{rand_idx:05d}",
                    victim_cust_id=f"cust_gen_{rand_idx}"
                ))

            # 4. Run High-Value Wire Fraud Injection Anomaly
            if loop_count % 12 == 0:
                rand_idx = random.randint(0, TARGET_NUM_CUSTOMERS - 1)
                asyncio.create_task(ScenarioManager.run_wire_fraud(
                    victim_account=f"ACC-GEN{rand_idx:05d}",
                    victim_cust_id=f"cust_gen_{rand_idx}"
                ))

            # 5. Run High-Threat Device Prediction Vector (Known Hardware Blacklist)
            if loop_count % 13 == 0:
                asyncio.create_task(ScenarioManager.run_known_device_threat_attack(TARGET_NUM_CUSTOMERS))

            # 6. Run Complex Money Laundering networks (Fan-In/Smurfing)
            if loop_count % 14 == 0:
                asyncio.create_task(ScenarioManager.run_mule_and_structuring_network(TARGET_NUM_CUSTOMERS))

            # 7. Run Rogue Employee Mass Ledger Scraping Event Signature
            if loop_count % 16 == 0:
                target_staff = random.choice(employee_pool)
                asyncio.create_task(ScenarioManager.run_insider_threat_scraping(target_staff.employee_id))

            # 8. Run Social Engineering / SIM Swap Chain Attack
            if loop_count % 20 == 0:
                rand_idx = random.randint(0, TARGET_NUM_CUSTOMERS - 1)
                asyncio.create_task(ScenarioManager.run_social_engineering_chain(
                    victim_account=f"ACC-GEN{rand_idx:05d}",
                    victim_cust_id=f"cust_gen_{rand_idx}"
                ))

            # 9. Run Privilege Abuse (Insider Targeting VIP Accounts)
            if loop_count % 22 == 0:
                target_staff = random.choice(employee_pool)
                asyncio.create_task(ScenarioManager.run_privilege_abuse(target_staff.employee_id))

            # 10. Run API Abuse Enumeration Probes
            if loop_count % 24 == 0:
                asyncio.create_task(ScenarioManager.run_api_abuse_and_enumeration())

            # 11. Run Malware Infection (Browser Fingerprint Mutation)
            if loop_count % 26 == 0:
                asyncio.create_task(ScenarioManager.run_malware_infected_customer(TARGET_NUM_CUSTOMERS))

            # 12. Run Infrastructure DDoS botnet stress tests
            if loop_count % 30 == 0:
                asyncio.create_task(ScenarioManager.run_cyber_infrastructure_attack())

if __name__ == "__main__":
    try:
        asyncio.run(main_engine())
    except KeyboardInterrupt:
        print("\n[Shutdown] Halting simulator scaling core pipeline... Goodbye!")
        sys.exit(0)