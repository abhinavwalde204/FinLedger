import json, math, hashlib, datetime
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from confluent_kafka import Consumer

KAFKA_CONF = {
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'flink-finledger',
    'auto.offset.reset': 'earliest'
}

MINIO_FS = s3fs.S3FileSystem(
    endpoint_url='http://localhost:9000',
    key='minioadmin',
    secret='minioadmin',
    use_ssl=False
)

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat/2)**2 +
         math.cos(math.radians(lat1)) *
         math.cos(math.radians(lat2)) *
         math.sin(dlon/2)**2)
    return R * 2 * math.asin(math.sqrt(max(0, min(1, a))))

def score_session(txns):
    amounts = [t['amount'] for t in txns]
    mean    = sum(amounts) / len(amounts)
    std     = (sum((x - mean)**2 for x in amounts) / len(amounts))**0.5 or 1
    z       = abs((max(amounts) - mean) / std)

    geo_speed = 0
    for i in range(len(txns) - 1):
        dist    = haversine(txns[i]['lat'], txns[i]['lon'],
                            txns[i+1]['lat'], txns[i+1]['lon'])
        elapsed = max((txns[i+1]['ts'] - txns[i]['ts']) / 3600000, 0.001)
        geo_speed = max(geo_speed, dist / elapsed)

    return {
        'account_key':         hashlib.sha256(
                                   txns[0]['account_id'].encode()
                               ).hexdigest()[:16],
        'session_txn_count':   len(txns),
        'total_amount':        round(sum(amounts), 2),
        'distinct_countries':  len(set(t['country'] for t in txns)),
        'max_geo_speed_kmph':  round(geo_speed, 2),
        'anomaly_score':       round(z, 4),
        'is_anomaly':          z > 2.5 or geo_speed > 900,
        'anomaly_type': (
            'GEO_IMPOSSIBLE' if geo_speed > 900 else
            'VELOCITY_BURST' if z > 2.5 else
            'NORMAL'
        ),
        'partition_date': datetime.date.today().isoformat()
    }

def write_to_minio(records):
    schema = pa.schema([
        ('account_key',        pa.string()),
        ('session_txn_count',  pa.int32()),
        ('total_amount',       pa.float64()),
        ('distinct_countries', pa.int32()),
        ('max_geo_speed_kmph', pa.float64()),
        ('anomaly_score',      pa.float64()),
        ('is_anomaly',         pa.bool_()),
        ('anomaly_type',       pa.string()),
        ('partition_date',     pa.string())
    ])
    table = pa.Table.from_pylist(records, schema=schema)
    today = datetime.date.today().isoformat()
    path  = (f"finledger-lake/bronze/sessions/dt={today}/"
             f"batch_{int(datetime.datetime.now().timestamp())}.parquet")
    with MINIO_FS.open(path, 'wb') as f:
        pq.write_table(table, f)
    print(f"Written to MinIO: {path}")

def parse(msg):
    try:
        d = json.loads(msg)
        p = d.get('payload', d)
        
        amt_raw = p.get('amount_usd', 0)
        if isinstance(amt_raw, str):
            # Debezium encodes DECIMAL as base64 big-endian integer
            # with scale factor from schema
            import base64
            raw_bytes = base64.b64decode(amt_raw)
            # pad to 8 bytes
            padded = raw_bytes.rjust(8, b'\x00')
            import struct
            raw_int = struct.unpack('>q', padded)[0]
            # Debezium uses scale=4 for DECIMAL(18,4)
            amt = raw_int / 10000.0
        else:
            amt = float(amt_raw or 0)

        return {
            'account_id': p.get('account_id', ''),
            'amount':     round(abs(amt), 2),
            'country':    p.get('geo_country', 'XX'),
            'lat':        float(p.get('lat', 0) or 0),
            'lon':        float(p.get('lon', 0) or 0),
            'ts':         int(p.get('__ts_ms', 0) or 0)
        }
    except Exception as e:
        print(f"Parse error: {e}")
        return None

def main():
    consumer = Consumer(KAFKA_CONF)
    consumer.subscribe(['cdc.public.transactions'])

    session_groups = {}
    SESSION_SIZE   = 5
    print("Listening for transactions...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue

            record = parse(msg.value().decode('utf-8'))
            if not record or not record['account_id']:
                continue

            aid = record['account_id']
            session_groups.setdefault(aid, []).append(record)

            if len(session_groups[aid]) >= SESSION_SIZE:
                group  = session_groups.pop(aid)
                result = score_session(group)
                print(f"SESSION SCORED: account={result['account_key']} "
                      f"txns={result['session_txn_count']} "
                      f"anomaly={result['is_anomaly']} "
                      f"type={result['anomaly_type']}")
                write_to_minio([result])

    except KeyboardInterrupt:
        print("Stopped.")
    finally:
        consumer.close()

if __name__ == '__main__':
    main()