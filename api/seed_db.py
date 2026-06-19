from app.core.database import SessionLocal
from app.models.domain import Employee, Customer, Account  # Added Account import

def seed_database():
    db = SessionLocal()
    
    try:
        # 1. Create the missing hardcoded employee
        emp = db.query(Employee).filter(Employee.employee_id == "emp_system_dev").first()
        if not emp:
            print("Creating emp_system_dev...")
            db.add(Employee(
                employee_id="emp_system_dev", 
                department="IT Support",
                role="System Administrator"
            ))

        # 2. Create the missing generic simulator customers AND their Accounts
        for i in range(21):
            cust_id = f"cust_gen_{i}"
            cust = db.query(Customer).filter(Customer.customer_id == cust_id).first()
            if not cust:
                print(f"Creating {cust_id} and Account ACC-GEN{i:05d}...")
                db.add(Customer(
                    customer_id=cust_id, 
                    first_name="Test", 
                    last_name=f"User {i}",
                    email=f"test{i}@bank.local",
                    country="US"
                ))
                # Explicitly seed the account matching the scenario generator
                db.add(Account(
                    account_id=f"ACC-GEN{i:05d}",
                    customer_id=cust_id,
                    balance=50000.00,
                    currency="USD",
                    status="ACTIVE"
                ))

        # 3. Create the VIP Customers for the Insider Threat Scenario
        vip_ids = ["VIP-CEO-ACCOUNT-001", "VIP-CFO-ACCOUNT-002"]
        for vip in vip_ids:
            cust = db.query(Customer).filter(Customer.customer_id == vip).first()
            if not cust:
                print(f"Creating VIP target: {vip}...")
                db.add(Customer(
                    customer_id=vip,
                    first_name="Executive",
                    last_name="VIP",
                    email=f"{vip.lower()}@bank.local",
                    country="US"
                ))

        db.commit()
        print("Database successfully seeded! You can now run the simulator.")
        
    except Exception as e:
        print(f"Error seeding database: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    seed_database()