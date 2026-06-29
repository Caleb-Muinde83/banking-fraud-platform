import httpx
import random
import asyncio
import uuid
import os
from faker import Faker

fake = Faker()
# Base URL targeting absolute root domain to prevent path-stripping
API_URL = os.getenv("API_URL", os.getenv("BANKING_API_URL", "http://127.0.0.1:8000"))

class ScenarioManager:
    
    # =========================================================================
    # TIER 1: ORIGINAL BASELINE SCENARIOS
    # =========================================================================

    @staticmethod
    async def run_credential_stuffing():
        print("\n[🚨 ATTACK ENGAGED] Spawning Credential Stuffing Campaign...")
        attacker_ip = "198.51.100.42"
        target_device = str(uuid.uuid4())
        
        async with httpx.AsyncClient(base_url=API_URL) as client:
            for _ in range(25):
                payload = {
                    "username": f"cust_gen_{random.randint(0, 20)}",
                    "password": "WrongPasswordAttempt99!",
                    "device_id": target_device,
                    "device_type": "DESKTOP",
                    "ip_address": attacker_ip,
                    "country": "CN"
                }
                try:
                    await client.post("/api/login", json=payload)
                except httpx.RequestError:
                    pass
        print("[🚨 ATTACK RUN COMPLETE] Credential stuffing signatures pushed to pipeline.")

    @staticmethod
    async def run_account_takeover(victim_account="ACC-GEN00000", victim_cust_id="cust_gen_0"):
        print(f"\n[🚨 ATTACK ENGAGED] Initiating Account Takeover Chain on {victim_cust_id}...")
        rogue_headers = {"X-User-Id": victim_cust_id}
        
        async with httpx.AsyncClient(base_url=API_URL) as client:
            try:
                await client.post("/api/login", json={
                    "username": victim_cust_id,
                    "password": "SecurePassword123!",
                    "device_id": str(uuid.uuid4()),
                    "device_type": "MOBILE",
                    "ip_address": "185.220.101.5",
                    "country": "RU"
                }, headers=rogue_headers)
                await asyncio.sleep(0.5)
                
                mule_acc = "ACC-MULE-ERR9"
                await client.post("/api/beneficiaries", json={
                    "account_number": mule_acc,
                    "bank_name": "Offshore Exfiltration Bank"
                }, headers=rogue_headers)
                await asyncio.sleep(0.5)
                
                await client.post("/api/transfers", json={
                    "from_account": victim_account,
                    "to_account": mule_acc,
                    "amount": 980.00,
                    "currency": "USD",
                    "transaction_type": "TRANSFER"
                }, headers=rogue_headers)
                print(f"[🚨 ATTACK RUN COMPLETE] ATO drainage completed on {victim_account}.")
            except httpx.RequestError:
                pass

    @staticmethod
    async def run_wire_fraud(victim_account="ACC-GEN00001", victim_cust_id="cust_gen_1"):
        print(f"\n[🚨 ATTACK ENGAGED] Executing Wire Fraud Exploit on {victim_account}...")
        headers = {"X-User-Id": victim_cust_id}
        fraud_mule_acc = f"ACC-MULE-{random.randint(100, 999)}"
        
        async with httpx.AsyncClient(base_url=API_URL) as client:
            try:
                await client.post("/api/beneficiaries", json={
                    "account_number": fraud_mule_acc, 
                    "bank_name": "Shadow Clearance Corp"
                }, headers=headers)
                
                await client.post("/api/transfers", json={
                    "from_account": victim_account,
                    "to_account": fraud_mule_acc,
                    "amount": 25000.00,
                    "currency": "USD",
                    "transaction_type": "TRANSFER"
                }, headers=headers)
                print("[🚨 ATTACK RUN COMPLETE] Wire fraud transfer injected.")
            except httpx.RequestError:
                pass

    @staticmethod
    async def run_insider_threat_scraping(malicious_employee_id="EMP-ROUGE-99"):
        print(f"\n[🚨 ATTACK ENGAGED] Insider Threat: Mass Ledger Scraping by Staff ID {malicious_employee_id}...")
        rogue_headers = {"X-Employee-Id": malicious_employee_id, "X-Employee-Role": "TELLER"}
        
        async with httpx.AsyncClient(base_url=API_URL) as client:
            for _ in range(40):  
                fake_target = f"cust_gen_{random.randint(0, 20)}"
                try:
                    await client.post("/api/employee/view-account", json={"customer_id": fake_target}, headers=rogue_headers)
                except httpx.RequestError:
                    pass
        print("[🚨 ATTACK RUN COMPLETE] Insider threat scraping finished.")

    # =========================================================================
    # TIER 2: ADVANCED COMPOSITE PATTERNS & SOCIAL ENGINEERING
    # =========================================================================

    @staticmethod
    async def run_auth_abuse_campaign(total_generated_users=21):
        print("\n[🚨 ATTACK ENGAGED] Launching Distributed Authentication Abuse Campaign...")
        attacker_ip = "203.0.113.88"
        attacker_device = str(uuid.uuid4())
        
        async with httpx.AsyncClient(base_url=API_URL) as client:
            for _ in range(30):
                rand_idx = random.randint(0, total_generated_users - 1)
                try:
                    await client.post("/api/login", json={
                        "username": f"cust_gen_{rand_idx}",
                        "password": "Spring2026!@#",
                        "device_id": attacker_device,
                        "ip_address": attacker_ip,
                        "country": "UA"
                    })
                except httpx.RequestError:
                    pass

            target_victim = f"cust_gen_{random.randint(0, total_generated_users - 1)}"
            for _ in range(15):
                try:
                    await client.post("/api/mfa/request", json={"username": target_victim})
                except httpx.RequestError:
                    pass
        print("[🚨 ATTACK RUN COMPLETE] Auth abuse campaign finished.")

    @staticmethod
    async def run_mule_and_structuring_network(total_generated_users=21):
        print("\n[🚨 ATTACK ENGAGED] Initiating Multi-Node AML Layering & Smurfing Network...")
        central_mule_acc = "ACC-CENTRAL-MULE-99"
        
        async with httpx.AsyncClient(base_url=API_URL) as client:
            for _ in range(5):
                rand_idx = random.randint(0, total_generated_users - 1)
                try:
                    await client.post("/api/transfers", json={
                        "from_account": f"ACC-GEN{rand_idx:05d}",
                        "to_account": central_mule_acc,
                        "amount": round(random.uniform(800.0, 1200.0), 2),
                        "currency": "USD",
                        "transaction_type": "TRANSFER"
                    }, headers={"X-User-Id": f"cust_gen_{rand_idx}"})
                except httpx.RequestError:
                    pass

            target_smurf_acc = f"ACC-GEN{random.randint(0, total_generated_users - 1):05d}"
            for _ in range(6):
                try:
                    await client.post("/api/transfers", json={
                        "from_account": central_mule_acc,
                        "to_account": target_smurf_acc,
                        "amount": 9500.00, 
                        "currency": "USD",
                        "transaction_type": "TRANSFER"
                    }, headers={"X-User-Id": "mule_operator_central"})
                except httpx.RequestError:
                    pass
        print("[🚨 ATTACK RUN COMPLETE] Smurfing network execution finished.")

    @staticmethod
    async def run_api_abuse_and_enumeration():
        print("\n[🚨 ATTACK ENGAGED] Spawning API Enumeration & Verification Scraper...")
        async with httpx.AsyncClient(base_url=API_URL) as client:
            for base_id in range(10100, 10130):
                headers = {
                    "X-Forwarded-For": f"192.0.2.{random.randint(1, 254)}",
                    "X-Device-Id": str(uuid.uuid4())
                }
                try:
                    await client.get(f"/api/accounts/ACC-GEN{base_id}", headers=headers)
                except httpx.RequestError:
                    pass
        print("[🚨 ATTACK RUN COMPLETE] API Enumeration finished.")

    @staticmethod
    async def run_social_engineering_chain(victim_account="ACC-GEN00002", victim_cust_id="cust_gen_2"):
        print(f"\n[🚨 ATTACK ENGAGED] Executing Social Engineering / SIM Swap Chain on {victim_cust_id}...")
        headers = {"X-User-Id": victim_cust_id}
        
        async with httpx.AsyncClient(base_url=API_URL) as client:
            try:
                await client.post("/api/user/update-profile", json={
                    "customer_id": victim_cust_id,
                    "phone": f"+1-555-{random.randint(100, 999)}-0199",
                    "email": "hijacked_gateway@darknet.io"
                }, headers=headers)
                await asyncio.sleep(0.5)

                await client.post("/api/auth/reset-password", json={"customer_id": victim_cust_id})
                await asyncio.sleep(0.5)

                await client.post("/api/transfers", json={
                    "from_account": victim_account,
                    "to_account": "ACC-MULE-SOCENG",
                    "amount": 4850.00,
                    "currency": "USD",
                    "transaction_type": "TRANSFER"
                }, headers=headers)
                print("[🚨 ATTACK RUN COMPLETE] Social engineering chain finished.")
            except httpx.RequestError:
                pass

    @staticmethod
    async def run_cyber_infrastructure_attack():
        print("\n[🚨 ATTACK ENGAGED] Triggering Botnet DDoS Flood on Authentication Gateways...")
        async with httpx.AsyncClient(base_url=API_URL) as client:
            async def single_bot_strike():
                bot_ip = f"172.16.{random.randint(0,255)}.{random.randint(1,254)}"
                try:
                    await client.post("/api/login", json={
                        "username": f"cust_gen_{random.randint(0, 20)}",
                        "password": "BruteForceAttackStrikes99!",
                        "ip_address": bot_ip
                    })
                except httpx.RequestError:
                    pass

            bot_tasks = [asyncio.create_task(single_bot_strike()) for _ in range(50)]
            await asyncio.gather(*bot_tasks, return_exceptions=True)
        print("[🚨 ATTACK RUN COMPLETE] DDoS Flood payload delivered.")

    # =========================================================================
    # TIER 3: INFRASTRUCTURE, HARDWARE & BEHAVIORAL THREATS
    # =========================================================================
    
    KNOWN_HIGH_THREAT_DEVICES = [
        "dev-fraud-emulator-root-01",
        "dev-cloned-session-spoof-99",
        "dev-malware-infected-bot-42"
    ]

    @staticmethod
    async def run_known_device_threat_attack(total_generated_users=21):
        rogue_device = random.choice(ScenarioManager.KNOWN_HIGH_THREAT_DEVICES)
        rand_idx = random.randint(0, total_generated_users - 1)
        victim_user = f"cust_gen_{rand_idx}"
        
        print(f"\n[🚨 ATTACK ENGAGED] Exploit Attempt using Blacklisted Device: {rogue_device}...")
        async with httpx.AsyncClient(base_url=API_URL) as client:
            try:
                await client.post("/api/login", json={
                    "username": victim_user,
                    "password": "ValidOrGuessedPassword123!",
                    "device_id": rogue_device,
                    "device_type": "EMULATOR",
                    "ip_address": f"198.51.100.{random.randint(1, 254)}",
                    "country": "US"
                }, headers={"X-User-Id": victim_user})
                
                await asyncio.sleep(0.5)
                await client.post("/api/transfers", json={
                    "from_account": f"ACC-GEN{rand_idx:05d}",
                    "to_account": "ACC-MULE-DEVICE-DROP",
                    "amount": 2500.00,
                    "currency": "USD",
                    "transaction_type": "TRANSFER"
                }, headers={"X-User-Id": victim_user, "X-Device-Id": rogue_device})
                print("[🚨 ATTACK RUN COMPLETE] Device Threat attack delivered.")
            except httpx.RequestError:
                pass

    @staticmethod
    async def run_privilege_abuse(malicious_employee_id="EMP-ROUGE-99"):
        """Scenario 8: Teller out of bounds accessing Executive/VIP accounts."""
        print(f"\n[🚨 ATTACK ENGAGED] Privilege Abuse: Employee {malicious_employee_id} accessing VIP accounts...")
        rogue_headers = {"X-Employee-Id": malicious_employee_id, "X-Employee-Role": "TELLER"}
        
        async with httpx.AsyncClient(base_url=API_URL) as client:
            try:
                await client.post("/api/employee/view-account", json={"customer_id": "VIP-CEO-ACCOUNT-001"}, headers=rogue_headers)
                await asyncio.sleep(0.5)
                await client.post("/api/employee/view-account", json={"customer_id": "VIP-CFO-ACCOUNT-002"}, headers=rogue_headers)
                print("[🚨 ATTACK RUN COMPLETE] Privilege abuse logs injected into pipeline.")
            except httpx.RequestError:
                pass

    @staticmethod
    async def run_malware_infected_customer(total_generated_users=21):
        """Scenario 21: Legitimate user transaction interrupted by sudden browser fingerprint mutation."""
        rand_idx = random.randint(0, total_generated_users - 1)
        victim_user = f"cust_gen_{rand_idx}"
        victim_acc = f"ACC-GEN{rand_idx:05d}"
        
        print(f"\n[🚨 ATTACK ENGAGED] Malware Session Hijack Sequence on user: {victim_user}...")
        
        async with httpx.AsyncClient(base_url=API_URL) as client:
            try:
                normal_headers = {
                    "X-User-Id": victim_user,
                    "X-Browser-Fingerprint": "Standard-Chrome-Win10-Hash-88A92"
                }
                await client.get(f"/api/accounts/{victim_acc}", headers=normal_headers)
                await asyncio.sleep(1)
                
                malware_headers = {
                    "X-User-Id": victim_user,
                    "X-Browser-Fingerprint": "Malware-Hook-Headless-Hash-XYZ99"
                }
                await client.post("/api/transfers", json={
                    "from_account": victim_acc,
                    "to_account": "ACC-MULE-MALWARE-DROP",
                    "amount": 2999.00,
                    "currency": "USD",
                    "transaction_type": "TRANSFER"
                }, headers=malware_headers)
                print(f"[🚨 ATTACK RUN COMPLETE] Fingerprint mutation & hidden transfer triggered on {victim_user}.")
            except httpx.RequestError:
                pass

    @staticmethod
    async def run_money_laundering():
        print("\n[🚨 ATTACK ENGAGED] Initiating Money Laundering Layering Sequence...")
        chain = ["cust_gen_1", "cust_gen_2", "cust_gen_3", "cust_gen_4"]
        accounts = ["ACC-GEN00001", "ACC-GEN00002", "ACC-GEN00003", "ACC-GEN00004"]
        
        amount = 9500.00
        
        async with httpx.AsyncClient(base_url=API_URL) as client:
            for i in range(len(chain) - 1):
                sender = chain[i]
                sender_acc = accounts[i]
                receiver_acc = accounts[i+1]
                
                print(f" -> Layering Step {i+1}: Transferring ${amount} from {sender_acc} to {receiver_acc}")
                try:
                    await client.post("/api/transfers", json={
                        "from_account": sender_acc,
                        "to_account": receiver_acc,
                        "amount": amount,
                        "currency": "USD",
                        "transaction_type": "TRANSFER"
                    }, headers={"X-User-Id": sender})
                except httpx.RequestError:
                    pass
                
                amount -= random.uniform(50, 200)
                await asyncio.sleep(1)
                
        print("[🚨 ATTACK RUN COMPLETE] Money Laundering layering sequence complete.")

    @staticmethod
    async def run_fraud_ring():
        print("\n[🚨 ATTACK ENGAGED] Setting up Fraud Ring infrastructure...")
        shared_ip = "45.33.22.11"
        shared_device = str(uuid.uuid4())
        ring_members = ["cust_gen_5", "cust_gen_6", "cust_gen_7"]
        ring_accounts = ["ACC-GEN00005", "ACC-GEN00006", "ACC-GEN00007"]

        async with httpx.AsyncClient(base_url=API_URL) as client:
            print(" -> Step 1: Ring members authenticating from shared infrastructure")
            for member in ring_members:
                try:
                    await client.post("/api/login", json={
                        "username": member,
                        "password": "StandardSecureUserPassword123!",
                        "device_id": shared_device,
                        "device_type": "DESKTOP",
                        "ip_address": shared_ip,
                        "country": "US"
                    })
                except httpx.RequestError:
                    pass
                
            print(" -> Step 2: Executing circular fund transfers")
            for i in range(len(ring_members)):
                sender = ring_members[i]
                sender_acc = ring_accounts[i]
                receiver_acc = ring_accounts[(i + 1) % len(ring_members)]
                
                try:
                    await client.post("/api/transfers", json={
                        "from_account": sender_acc,
                        "to_account": receiver_acc,
                        "amount": 1200.00,
                        "currency": "USD",
                        "transaction_type": "TRANSFER"
                    }, headers={"X-User-Id": sender})
                except httpx.RequestError:
                    pass
                
        print("[🚨 ATTACK RUN COMPLETE] Fraud Ring activity complete.")

    @staticmethod
    async def run_ransomware():
        print("\n[🚨 ATTACK ENGAGED] Simulating Ransomware DB Encryption/Deletion Spikes...")
        async with httpx.AsyncClient(base_url=API_URL) as client:
            tasks = []
            for _ in range(50):
                fake_session_id = str(uuid.uuid4())
                tasks.append(client.delete(f"/api/sessions/{fake_session_id}"))
                
            await asyncio.gather(*tasks, return_exceptions=True)
        print("[🚨 ATTACK RUN COMPLETE] Ransomware API deletion spike anomaly complete.")
