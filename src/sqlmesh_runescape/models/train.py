from datetime import datetime
from typing import Any

import pandas as pd

from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName


MODEL_NAME = "sqlmesh_example.train"

MIN_CUST_COST = 1
MAX_RET = 5


@model(
    MODEL_NAME,
    columns={
        "ts": "timestamp",
        "lowalch": "float",
        "highalch": "float",
        "id": "int",
        "avg_high_price": "float",
        "avg_low_price": "float",
        "high_price_volume": "float",
        "low_price_volume": "float",
    },
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE, time_column="ts"
    ),  # , batch_size=24, batch_concurrency=1),
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
    # With a python model, you need to specify the where clause, unlike sql models. Makes sense,
    # and they give you the start and end values so it's easy.
    WHERE_CLAUSE = f"where ts >= TIMESTAMP '{start}' and ts <= TIMESTAMP '{end}'"
    mapping_df = context.fetchdf(f"select lowalch, highalch, id from {mapping}")
    hourly_scrape_df = context.fetchdf(f"select * from {hourly_scrape} {WHERE_CLAUSE}")
    df = hourly_scrape_df.merge(mapping_df, how="left", on="id")
    # TODO: Let's do something cool with pandas/polars here
    return df

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
