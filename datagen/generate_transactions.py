import psycopg2, uuid, random, time
from faker import Faker
fake = Faker()

conn = psycopg2.connect(
    host="localhost", dbname="finledger_oltp",
    user="admin", password="secret"
)
cur = conn.cursor()

COUNTRIES = ["IN","US","GB","SG","AE","DE","FR","JP","AU","BR"]
MCC       = ["5411","5812","4111","7011","5945","5999","4900"]
TYPES     = ["DEBIT","CREDIT","TRANSFER"]
CHANNELS  = ["MOBILE","WEB","ATM","POS"]

customer_ids, account_ids = [], []

for _ in range(500):
    cid = str(uuid.uuid4())
    customer_ids.append(cid)
    cur.execute(
        "INSERT INTO customers VALUES(%s,%s,%s,%s,NOW())",
        (cid, fake.name()[:100], fake.email()[:100], fake.phone_number()[:20])
    )
    aid = str(uuid.uuid4())
    account_ids.append(aid)
    cur.execute(
        "INSERT INTO accounts VALUES(%s,%s,%s,%s,%s,%s,NOW())",
        (aid, cid,
         random.choice(["SAVINGS","CURRENT","CREDIT"]),
         round(random.uniform(10000, 500000), 2),
         random.choice(["LOW","MED","HIGH"]),
         random.choice(["VERIFIED","PENDING"]))
    )

conn.commit()
print("Seeded 500 customers and accounts. Streaming transactions...")

while True:
    aid = random.choice(account_ids)
    cur.execute(
        """INSERT INTO transactions VALUES
           (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())""",
        (str(uuid.uuid4()), aid, str(uuid.uuid4()),
         fake.company()[:100], random.choice(MCC),
         round(random.uniform(1, 50000), 2),
         random.choice(TYPES), random.choice(CHANNELS),
         random.choice(COUNTRIES),
         round(random.uniform(-90, 90), 4),
         round(random.uniform(-180, 180), 4))
    )
    conn.commit()
    time.sleep(0.5)