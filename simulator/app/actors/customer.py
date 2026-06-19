import random
import asyncio
import httpx
from app.utils.generators import generate_device_fingerprint, generate_network_profile, generate_random_account_id

# =========================================================================
# CUSTOMER ACTOR (Generates Baseline Legitimate Traffic)
# =========================================================================

class AsyncCustomerActor:
    def __init__(self, username, account_id, persona_type, fixed_country):
        self.username = username
        self.account_id = account_id
        self.persona_type = persona_type  # "STUDENT", "BUSINESS", "PROFESSIONAL", or "RETIREE"
        
        # Consistent telemetry variables per agent profile instance
        self.device = generate_device_fingerprint()
        self.network = generate_network_profile(fixed_country=fixed_country)
        self.is_logged_in = False

    async def perform_action(self, client: httpx.AsyncClient):
        """Executes a randomized profile task over a non-blocking asynchronous event loop context."""
        if not self.is_logged_in:
            await self._execute_login(client)
            if not self.is_logged_in:
                return  # Terminate turn cycle early if session handshake drops

        # Distribution matrix matching spec definitions (Section 7)
        action_pool = ["LOOK_AT_BALANCE", "MAKE_TRANSFER", "ADD_BENEFICIARY"]
        
        if self.persona_type == "STUDENT":
            weights = [0.75, 0.20, 0.05]  
        elif self.persona_type == "BUSINESS":                
            weights = [0.30, 0.55, 0.15]  
        elif self.persona_type == "PROFESSIONAL":
            weights = [0.45, 0.45, 0.10]
        else: # RETIREE
            weights = [0.55, 0.40, 0.05]

        chosen_action = random.choices(action_pool, weights=weights)[0]

        if chosen_action == "LOOK_AT_BALANCE":
            await self._execute_clear_balance_check(client)
        elif chosen_action == "MAKE_TRANSFER":
            await self._execute_valid_transfer(client)
        elif chosen_action == "ADD_BENEFICIARY":
            await self._execute_add_beneficiary(client)

    async def _execute_login(self, client: httpx.AsyncClient):
        payload = {
            "username": self.username,
            "password": "StandardSecureUserPassword123!",
            "device_id": self.device["device_id"],
            "device_type": self.device["device_type"],
            "ip_address": self.network["ip_address"],
            "country": self.network["country"]
        }
        try:
            # Explicitly prefixing with /api to guarantee accurate backend routing
            r = await client.post("/api/login", json=payload)
            if r.status_code == 200:
                self.is_logged_in = True
        except httpx.RequestError:
            pass

    async def _execute_clear_balance_check(self, client: httpx.AsyncClient):
        try:
            await client.get(f"/api/accounts/{self.account_id}/balance", headers={"X-User-Id": self.username})
        except httpx.RequestError:
            pass

    async def _execute_valid_transfer(self, client: httpx.AsyncClient):
        if self.persona_type == "STUDENT":
            amount = round(random.uniform(5.00, 45.00), 2)
        elif self.persona_type == "BUSINESS":
            amount = round(random.uniform(500.00, 8500.00), 2)
        elif self.persona_type == "PROFESSIONAL":
            amount = round(random.uniform(40.00, 600.00), 2)
        else:
            amount = round(random.uniform(50.00, 300.00), 2)

        destination_target = generate_random_account_id()
        payload = {
            "from_account": self.account_id,
            "to_account": destination_target,
            "amount": amount,
            "currency": "USD",
            "transaction_type": "TRANSFER"
        }
        try:
            await client.post("/api/transfers", json=payload, headers={"X-User-Id": self.username})
        except httpx.RequestError:
            pass

    async def _execute_add_beneficiary(self, client: httpx.AsyncClient):
        payload = {
            "account_number": generate_random_account_id(),
            "bank_name": random.choice(["Global Central Bank", "Nexus Financial", "Apex Trust", "Horizon Credit Union"])
        }
        try:
            await client.post("/api/beneficiaries", json=payload, headers={"X-User-Id": self.username})
        except httpx.RequestError:
            pass


# =========================================================================
# EMPLOYEE ACTOR (Generates Baseline Staff Operations Traffic)
# =========================================================================

class AsyncEmployeeActor:
    def __init__(self, employee_id, role="TELLER", department="RETAIL_BANKING"):
        self.employee_id = employee_id
        self.role = role
        self.department = department

    async def perform_action(self, client: httpx.AsyncClient):
        """Simulates standard corporate access operations within typical boundaries."""
        rogue_headers = {
            "X-Employee-Id": self.employee_id, 
            "X-Employee-Role": self.role,
            "X-Employee-Department": self.department
        }
        
        target_customer = f"cust_gen_{random.randint(0, 20)}"
        try:
            await client.post("/api/employee/view-account", json={"customer_id": target_customer}, headers=rogue_headers)
        except httpx.RequestError:
            pass