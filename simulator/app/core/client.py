import os
import httpx

BANKING_API_URL = os.getenv("BANKING_API_URL", "http://127.0.0.1:8000")

class BankClient:
    def __init__(self):
        # Establish an HTTP client session with standard connection timeouts
        self.client = httpx.Client(base_url=BANKING_API_URL, timeout=5.0)

    def login(self, username, password, device_id, device_type, ip_address, country):
        payload = {
            "username": username,
            "password": password,
            "device_id": device_id,
            "device_type": device_type,
            "ip_address": ip_address,
            "country": country
        }
        try:
            response = self.client.post("/api/login", json=payload)
            return response
        except httpx.RequestError as e:
            print(f"[Client Error] Failed to connect to Banking API for login: {e}")
            return None

    def check_balance(self, account_id):
        try:
            return self.client.get(f"/api/accounts/{account_id}/balance")
        except httpx.RequestError as e:
            print(f"[Client Error] Failed to fetch balance for {account_id}: {e}")
            return None

    def send_transfer(self, from_account, to_account, amount):
        payload = {
            "from_account": from_account,
            "to_account": to_account,
            "amount": float(amount),
            "currency": "USD",
            "transaction_type": "TRANSFER"
        }
        try:
            return self.client.post("/api/transfers", json=payload)
        except httpx.RequestError as e:
            print(f"[Client Error] Failed to execute transfer from {from_account}: {e}")
            return None