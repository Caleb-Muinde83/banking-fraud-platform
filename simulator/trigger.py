import asyncio
import sys

# Import the updated ScenarioManager mapping to your app.attacks folder structure
from app.attacks.scenarios import ScenarioManager

async def main():
    # Dictionary mapping CLI commands to the ScenarioManager static methods
    scenarios = {
        "credential_stuffing": ScenarioManager.run_credential_stuffing,
        "account_takeover": ScenarioManager.run_account_takeover,
        "wire_fraud": ScenarioManager.run_wire_fraud,
        "insider_scraping": ScenarioManager.run_insider_threat_scraping,
        "auth_abuse": ScenarioManager.run_auth_abuse_campaign,
        "aml_smurfing": ScenarioManager.run_mule_and_structuring_network,
        "api_enumeration": ScenarioManager.run_api_abuse_and_enumeration,
        "social_engineering": ScenarioManager.run_social_engineering_chain,
        "ddos_flood": ScenarioManager.run_cyber_infrastructure_attack,
        "known_threat_device": ScenarioManager.run_known_device_threat_attack,
        "privilege_abuse": ScenarioManager.run_privilege_abuse,
        "malware_hijack": ScenarioManager.run_malware_infected_customer,
        "money_laundering": ScenarioManager.run_money_laundering,
        "fraud_ring": ScenarioManager.run_fraud_ring,
        "ransomware": ScenarioManager.run_ransomware
    }

    if len(sys.argv) < 2 or sys.argv[1] not in scenarios:
        print("\n=== 🔴 BANKING FRAUD SIMULATOR: ATTACK TRIGGER ===")
        print("Usage: python trigger.py [scenario_name]\n")
        print("Available Scenarios:")
        for key in scenarios.keys():
            print(f"  - {key}")
        print("==================================================\n")
        sys.exit(1)

    attack_name = sys.argv[1]
    attack_method = scenarios[attack_name]
    
    # Execute the chosen async static method
    await attack_method()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[!] Attack aborted by user.")