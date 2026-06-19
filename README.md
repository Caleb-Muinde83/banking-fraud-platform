# Banking Fraud Platform

A local banking fraud simulation platform with core banking API, Kafka/Postgres infrastructure, and a transaction simulator.

## Quick Start

### 1. Start the container stack

From the project root:

```powershell
cd F:\DaTech-kafka\banking-fraud-platform
docker compose up -d
```

The stack starts:
- `postgres` on `localhost:5433`
- `kafka` on `localhost:9092`
- `connect` on `localhost:8083`

The root `.env` file contains required settings for `docker compose`.

To stop the stack:

```powershell
docker compose down
```

### 2. Run the API

From the `api` folder:

```powershell
cd api
venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

The API will be available at:

- `http://localhost:8000`

### 3. Run the simulator

From the `simulator` folder:

```powershell
cd simulator
venv\Scripts\activate
pip install -r requirements.txt
python trigger.py credential_stuffing
```

Replace `credential_stuffing` with any available scenario name.

#### Example simulator scenarios

- `credential_stuffing`
- `account_takeover`
- `wire_fraud`
- `insider_scraping`
- `auth_abuse`
- `aml_smurfing`
- `api_enumeration`
- `social_engineering`
- `ddos_flood`
- `known_threat_device`
- `privilege_abuse`
- `malware_hijack`
- `money_laundering`
- `fraud_ring`
- `ransomware`

## Project Structure

```text
.
├── .env
├── .gitignore
├── api/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── seed_db.py
│   └── app/
│       ├── main.py
│       ├── core/
│       ├── api/
│       ├── models/
│       └── schemas/
├── backend/
│   └── app/
├── debezium-postgres-connector.json
├── docker-compose.yml
├── k8s/
└── simulator/
    ├── Dockerfile
    ├── requirements.txt
    ├── trigger.py
    └── app/
```

## Notes

- Make sure local ports `5433`, `9092`, and `8083` are available.
- For Windows, use `venv\Scripts\activate` to activate the Python venv.
- Do not commit production secrets.
