# FinLedger
Builds an end-to-end ELT pipeline that ingests raw transaction logs from multiple mock banking cores, enforces GDPR/PCI-DSS compliance controls (masking, lineage, access audit), models a temporal snowflake schema for regulatory reporting, and uses a statistical anomaly scorer to flag suspicious transaction patterns in near-real-time.
