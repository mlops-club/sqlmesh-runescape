from datetime import datetime
from typing import Any

import pandas as pd

import duckdb
import polars as pl
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName


MODEL_NAME = "sqlmesh_example.train"

MIN_CUST_COST = 1
MAX_RET = 5


@model(
    MODEL_NAME,
    columns={
        "id": "int",
        "timestamp": "datetime",
        "prediction": "bool",
    },
    kind=dict(name=ModelKindName.INCREMENTAL_BY_TIME_RANGE, time_column="timestamp"),#, batch_size=24, batch_concurrency=1),
    # interval_unit="hour",
    cron="@hourly",
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: Any,
) -> pd.DataFrame:
    """Create the training table with prediction col."""
    mapping = context.table("sqlmesh_example.runescape_mapping")
    hourly_scrape = context.table("sqlmesh_example.hourly_scrape")
    # WHERE_CLAUSE = f"where 'timestamp' >= TIMESTAMP '{start}' and 'timestamp' <= TIMESTAMP '{end}'"
    # con = duckdb.connect(db_uri)
    mapping_df = context.fetchdf(f"select lowalch, highalch, id from {mapping}")
    hourly_scrape_df = context.fetchdf(f"select * from {hourly_scrape}")# {WHERE_CLAUSE}")
    return mapping_df
    
    # merged = df1.join(df2, on=["part", "ts"])
    # merged = merged.with_columns(
    #     pl.when(merged["cust_cost_spread"] * custom_mult > MIN_CUST_COST, merged["mean_cust_retention"] < MAX_RET)
    #     .then(True)
    #     .otherwise(False)
    #     .alias("prediction")
    # )
    # yield from yield_df(merged)
    # Implicit post-statement
    # upsert_df_delta(merged, gold_delta_uri, MODEL_NAME)