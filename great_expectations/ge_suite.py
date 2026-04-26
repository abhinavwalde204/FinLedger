import great_expectations as gx
import duckdb
import pandas as pd

con = duckdb.connect('/workspaces/FinLedger/finledger_warehouse.duckdb')
df = con.execute("SELECT * FROM main.stg_sessions").df()
con.close()

context = gx.get_context()

ds = context.data_sources.add_pandas(name="sessions_source")
asset = ds.add_dataframe_asset(name="sessions_df")
batch_def = asset.add_batch_definition_whole_dataframe("sessions_batch")
batch = batch_def.get_batch(batch_parameters={"dataframe": df})

suite = context.suites.add(
    gx.core.ExpectationSuite(name="finledger_suite")
)

suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="account_key"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="anomaly_score"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="anomaly_score", min_value=0, max_value=50))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="session_txn_count", min_value=1, max_value=10000))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="total_amount_usd", min_value=0.01, max_value=1e9))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
    column="anomaly_type",
    value_set=["NORMAL", "VELOCITY_BURST", "GEO_IMPOSSIBLE"]
))

validation_result = batch.validate(suite)

print(f"\n=== GREAT EXPECTATIONS RESULTS ===")
print(f"Success: {validation_result.success}")
print(f"\nExpectation results:")
for result in validation_result.results:
    status = "PASS" if result.success else "FAIL"
    exp_type = result.expectation_config.type
    print(f"  [{status}] {exp_type}")