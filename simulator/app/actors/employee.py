import requests
import random

API_URL = "http://127.0.0.1:8000/api"

class EmployeeActor:
    def __init__(self, employee_id, department, role):
        self.employee_id = employee_id
        self.department = department  # e.g., "RETAIL_BANKING", "COMPLIANCE"
        self.role = role              # e.g., "TELLER", "MANAGER" (Section 4)
        self.headers = {
            "X-Employee-Id": self.employee_id,
            "X-Employee-Role": self.role
        }

    def perform_internal_action(self, target_customer_ids):
        """Simulates an employee performing normal daytime administrative duties (Section 8)."""
        print(f"\n[Staff Audit] Internal action triggered by {self.employee_id} ({self.role})...")
        
        chosen_cust = random.choice(target_customer_ids)
        action = random.choice(["VIEW", "VIEW", "MAINTENANCE"])

        if action == "VIEW":
            # Hits POST /api/employee/view-account (Section 5)
            try:
                r = requests.post(f"{API_URL}/employee/view-account", json={"customer_id": chosen_cust}, headers=self.headers)
                if r.status_code == 200:
                    print(f" -> Employee {self.employee_id} reviewed profile data for {chosen_cust}")
            except requests.RequestException:
                pass
        elif action == "MAINTENANCE":
            # Hits POST /api/employee/update-account (Section 5)
            try:
                payload = {"account_id": f"ACC-{chosen_cust.split('_')[-1].upper()}", "status": "ACTIVE"}
                r = requests.post(f"{API_URL}/employee/update-account", json=payload, headers=self.headers)
                if r.status_code == 200:
                    print(f" -> System maintenance update completed by Employee {self.employee_id}")
            except requests.RequestException:
                pass