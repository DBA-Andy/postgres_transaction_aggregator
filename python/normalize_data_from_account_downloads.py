import csv
from datetime import datetime
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_batch
import getpass

# ---------------------------
# Configuration
# ---------------------------

FILES = [
    # Wright-Patt (3 junk rows, 4th row is header)
    ("wpcu_truesaver.csv", "Wright-Patt", "TrueSaver", "Savings", 3, 1),
    ("wpcu_moneymarket.csv", "Wright-Patt", "Money Market", "Money Market", 3, 2),
    ("wpcu_easysaver.csv", "Wright-Patt", "EasySaver", "Savings", 3, 3),
    ("wpcu_checking.csv", "Wright-Patt", "Totally Fair Checking", "Checking", 3, 4),

    # Credit Union of Ohio
    ("cuoh_savings.csv", "CU of Ohio", "Savings", "Savings", 0, 5),
    ("cuoh_checking.csv", "CU of Ohio", "Checking", "Checking", 0, 6),

    # Pathways
    ("pathways_savings.csv", "Pathways", "Savings", "Savings", 0, 7),
    ("pathways_mc.csv", "Pathways", "Mastercard", "Credit Card", 0, 8),

    # Hopewell
    ("hopewell_savings.csv", "Hopewell CU", "Savings", "Savings", 0, 9),
    ("hopewell_checking.csv", "Hopewell CU", "Checking", "Checking", 0, 10),

    # Telhio
    ("telhio_billpay.csv", "Telhio CU", "Bill Pay", "Checking", 0, 11),
    ("telhio_funmoney.csv", "Telhio CU", "Fun Money", "Checking", 0, 12),
    ("telhio_vacationsavings.csv", "Telhio CU", "Vacation Savings", "Savings", 0, 13),
    ("telhio_moneymarket.csv", "Telhio CU", "Money Market", "Money Market", 0, 14),
    ("telhio_visa.csv", "Telhio CU", "Visa", "Credit Card", 0, 15),

    # Citi
    ("citi_costcoanywhere.csv", "Citi", "Visa", "Credit Card", 0, 16),
   

    # Chase
    ("chase_freedom.csv", "Chase", "Visa", "Credit Card", 0, 17),
    ("chase_prime.csv", "Chase", "Visa", "Credit Card", 0, 18),
]

ACCOUNTS_OUT = "accounts.csv"
TRANSACTIONS_OUT = "transactions.csv"

# ---------------------------
# Helpers
# ---------------------------

def parse_date(date_str):
    for fmt in (
        "%m/%d/%Y",  # 12/25/2025
        "%m/%d/%y",  # 12/25/25
        "%Y-%m-%d",  # 2025-12-25
    ):
        try:
            return datetime.strptime(date_str.strip(), fmt).date().isoformat()
        except ValueError:
            pass
    raise ValueError(f"Unrecognized date format: {date_str}")

def to_float(value):
    return float(value.replace("$", "").replace(",", "").strip())

# ---------------------------
# Main Processing
# ---------------------------

accounts = []
transactions = []
seen_account_ids = set()


for filename, institution, account_name, account_type, skip_before_header, account_id in FILES:
    path = Path(filename)
    if not path.exists():
        print(f"WARNING: {filename} not found, skipping.")
        continue
    
    if account_id in seen_account_ids:
        raise ValueError(f"Duplicate account_id detected: {account_id}")
    seen_account_ids.add(account_id)

    current_account_id = account_id
    accounts.append({
        "account_id": current_account_id,
        "institution": institution,
        "account_name": account_name,
        "account_type": account_type,
        "source_file": filename
    })

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)

        print (f'Processing file: {filename}')

        # Skip junk rows before header
        for _ in range(skip_before_header):
            next(reader, None)

        # Read header row
        headers = next(reader)
        header_map = {h.strip().lower(): i for i, h in enumerate(headers)}

        for row in reader:
            if not any(row):
                continue

            # ---------------- Wright-Patt ----------------
            if institution == "Wright-Patt":
                date = parse_date(row[header_map["date"]])

                debit_raw = row[header_map["amount debit"]].strip()
                credit_raw = row[header_map["amount credit"]].strip()

                debit_amt = to_float(debit_raw) if debit_raw else 0.0
                credit_amt = to_float(credit_raw) if credit_raw else 0.0

                if debit_amt != 0:
                    amount = -abs(debit_amt)
                elif credit_amt != 0:
                    amount = abs(credit_amt)
                else:
                    continue  # no financial impact

                description = row[header_map["memo"]]

            # ---------------- CU of Ohio ----------------
            elif institution == "CU of Ohio":
                date = parse_date(row[header_map["date"]])
                amount = to_float(row[header_map["amount"]])

                if row[header_map["type"]].lower() == "debit":
                    amount = -abs(amount)

                description = row[header_map["description"]]

            # ---------------- Pathways ----------------
            elif institution == "Pathways":
                date = parse_date(row[header_map["date"]])
                amount = to_float(row[header_map["amount"]])
                description = row[header_map["description"]]

            # ---------------- Hopewell ----------------
            elif institution == "Hopewell CU":
                date = parse_date(row[header_map["processed date"]])
                amount = to_float(row[header_map["amount"]])

                if row[header_map["credit or debit"]].lower() == "debit":
                    amount = -abs(amount)

                description = row[header_map["description"]]

            # ---------------- Telhio ----------------
            elif institution == "Telhio CU":
                if filename == 'telhio_visa.csv':
                    date = parse_date(row[header_map["date"]])
                    description = row[header_map["name"]]
                else:
                    date = parse_date(row[header_map["posting date"]])
                    description = row[header_map["description"]]
                amount = to_float(row[header_map["amount"]])
                
            # ---------------- Citi ----------------
            elif institution == "Citi":
                date = parse_date(row[header_map["date"]])
                description = row[header_map["description"]]
                
                debit_raw = row[header_map["debit"]].strip()
                credit_raw = row[header_map["credit"]].strip()

                debit_amt = to_float(debit_raw) if debit_raw else 0.0
                credit_amt = to_float(credit_raw) if credit_raw else 0.0

                if debit_amt != 0:
                    amount = -abs(debit_amt)
                elif credit_amt != 0:
                    amount = abs(credit_amt)
                else:
                    continue  # no financial impact
            # ---------------- Chase ----------------
            elif institution == "Chase":
                date = parse_date(row[header_map["transaction date"]])
                amount = to_float(row[header_map["amount"]])
                description = row[header_map["description"]]

            else:
                continue

            transactions.append({
                "account_id": current_account_id,
                "transaction_date": date,
                "amount": round(amount, 2),
                "transaction_type": account_type,
                "description": description
            })

# ---------------------------
# Write Outputs
# ---------------------------

with open(ACCOUNTS_OUT, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["account_id", "institution", "account_name", "account_type", "source_file"]
    )
    writer.writeheader()
    writer.writerows(accounts)

with open(TRANSACTIONS_OUT, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["account_id", "transaction_date", "amount", "transaction_type", "description"]
    )
    writer.writeheader()
    writer.writerows(transactions)

print("Done.")
print(f"Wrote {len(accounts)} accounts to {ACCOUNTS_OUT}")
print(f"Wrote {len(transactions)} transactions to {TRANSACTIONS_OUT}")

# ---------------------------
# Database Load
# ---------------------------

print("\nPostgreSQL connection info:")
db_host = input("Host: ").strip()
db_port = input("Port [5432]: ").strip() or "5432"
db_name = input("Database name: ").strip()
db_user = input("Username: ").strip()
db_password = getpass.getpass("Password: ")

conn = psycopg2.connect(
    host=db_host,
    port=db_port,
    dbname=db_name,
    user=db_user,
    password=db_password
)

conn.autocommit = False

try:
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE transactions_stage cascade;")
        cur.execute("TRUNCATE TABLE accounts_stage cascade;")

        print("Loading accounts table...")
        execute_batch(
            cur,
            """
            INSERT INTO accounts_stage (
                account_id,
                institution,
                account_name,
                account_type,
                source_file
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            [
                (
                    a["account_id"],
                    a["institution"],
                    a["account_name"],
                    a["account_type"],
                    a["source_file"]
                )
                for a in accounts
            ]
        )

        print("Loading transactions_stage table...")
        execute_batch(
            cur,
            """
            INSERT INTO transactions_stage (
                account_id,
                transaction_date,
                amount,
                transaction_type,
                description
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            [
                (
                    t["account_id"],
                    t["transaction_date"],
                    t["amount"],
                    t["transaction_type"],
                    t["description"]
                )
                for t in transactions
            ]
        )
        print ("Loading to non-staging tables.")
        cur.execute("CALL load_from_staging();")
    conn.commit()
    print("Database load completed successfully.")

except Exception as e:
    conn.rollback()
    raise

finally:
    conn.close()
