import random
from faker import Faker

fake = Faker()

# Real-world telemetry metrics
DEVICE_MODELS = [
    "Apple iPhone 15 Pro", "Samsung Galaxy S24", "Google Pixel 8",
    "Windows Desktop (Chrome)", "MacBook Pro (Safari)", "Apple iPad Air"
]

COMMON_COUNTRIES = ["US", "CA", "GB", "DE", "FR", "AU", "SG"]

def generate_device_fingerprint():
    """Generates a random physical device profile for a customer."""
    return {
        "device_id": f"dv_{fake.uuid4()[:8]}",
        "device_type": random.choice(DEVICE_MODELS)
    }

def generate_network_profile(fixed_country=None):
    """Generates random IP routing details, mimicking mobile or broadband networks."""
    country = fixed_country if fixed_country else random.choice(COMMON_COUNTRIES)
    return {
        "ip_address": fake.ipv4(),
        "country": country
    }

def generate_random_account_id():
    """Generates a standard mock international account number structure."""
    return f"ACC-{random.randint(100000, 999999)}"