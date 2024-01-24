from datetime import datetime
from typing import Optional

import pandas as pd
from fennel.datasets import dataset, pipeline, field, Dataset
from fennel.featuresets import feature, featureset, extractor
from fennel.lib.aggregate import Count
from fennel.lib.expectations import (
    expectations,
    expect_column_values_to_be_between,
)
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs, outputs
from fennel.lib.window import Window
from fennel.sources import source, Postgres, Snowflake, Kafka, Webhook

postgres = Postgres.get(name="my_rdbms")
warehouse = Snowflake.get(name="my_warehouse")
kafka = Kafka.get(name="my_kafka")
webhook = Webhook(name="fennel_webhook")

@dataset
@source(postgres.table("product", cursor="updated"), every="1m", tier="prod")
@source(webhook.endpoint("Product"), tier="dev")
@meta(owner="chris@fennel.ai", tags=["PII"])
class Product:
    product_id: int = field(key=True)
    seller_id: int
    price: float
    desc: Optional[str]
    last_modified: datetime = field(timestamp=True)

    # Powerful primitives like data expectations for data hygiene
    @expectations
    def get_expectations(cls):
        return [
            expect_column_values_to_be_between(
                column="price", min_value=1, max_value=1e4, mostly=0.95
            )
        ]

@meta(owner="eva@fennel.ai")
@source(kafka.topic("orders"), lateness="1h", tier="prod")
@source(webhook.endpoint("Order"), tier="dev")
@dataset
class Order:
    uid: int
    product_id: int
    timestamp: datetime

@meta(owner="mark@fennel.ai")
@dataset
class UserSellerOrders:
    uid: int = field(key=True)
    seller_id: int = field(key=True)
    num_orders_1d: int
    num_orders_1w: int
    timestamp: datetime

    @pipeline(version=1)
    @inputs(Order, Product)
    def my_pipeline(cls, orders: Dataset, products: Dataset):
        orders = orders.join(products, how="left", on=["product_id"])
        orders = orders.transform(lambda df: df.fillna(0))
        orders = orders.drop("product_id", "desc", "price")
        orders = orders.dropnull()
        return orders.groupby("uid", "seller_id").aggregate(
            Count(window=Window("1d"), into_field="num_orders_1d"),
            Count(window=Window("1w"), into_field="num_orders_1w"),
        )

@meta(owner="nikhil@fennel.ai")
@featureset
class UserSellerFeatures:
    uid: int = feature(id=1)
    seller_id: int = feature(id=2)
    num_orders_1d: int = feature(id=3)
    num_orders_1w: int = feature(id=4)

    @extractor(depends_on=[UserSellerOrders])
    @inputs(uid, seller_id)
    @outputs(num_orders_1d, num_orders_1w)
    def myextractor(cls, ts: pd.Series, uids: pd.Series, sellers: pd.Series):
        df, found = UserSellerOrders.lookup(ts, seller_id=sellers, uid=uids)
        df = df.fillna(0)
        df["num_orders_1d"] = df["num_orders_1d"].astype(int)
        df["num_orders_1w"] = df["num_orders_1w"].astype(int)
        return df[["num_orders_1d", "num_orders_1w"]]