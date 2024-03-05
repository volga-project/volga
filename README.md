## Volga - Data engine for real-time AI/ML

Volga is a scalable data engine tailored for modern real-time AI/ML applications.
It features convenient pandas-like API to define data entities, online/offline pipelines and sources, 
consistent online+offline feature calculation semantics, plugable and configurable hot and cold storage, feature lookups, 
real-time serving and on-demand request-time calculations.
It is a completely standalone system removing any heavy-weight dependencies on data processors (Flink, Spark) and 
can be run on a laptop or on a 1000-node cluster

Features:

- Utilizes *[custom scalable stream processing engine](https://github.com/anovv/volga/blob/master/volga/streaming/runtime/master/test/test_e2e.py)* using **[Ray Actors](https://docs.ray.io/en/latest/ray-core/actors.html)** and **[ZeroMQ](https://zeromq.org/)**. 
Kappa architecture - no Flink or Spark
- Built on top of **[Ray](https://github.com/ray-project/ray)** - Easily integrates with Ray ecosystem 
(model training/serving, zero-copy data transfers, etc.) as well as your custom ML infrastructure
- Kubernetes ready, no vendor-lock - use **[KubeRay](https://github.com/ray-project/kuberay)** to run multitenant scalable jobs or create your own deployment/scheduling logic in pure Python
- Pure Python, no heavy JVM setups - minimal setup and maintenance efforts in production
- Standalone - launch on your laptop or a cluster with no heavy-weight external dependencies
- Cold + Hot storage model allows for customizable offline storage and caches, fast real-time feature queries
- Easy to use declarative pandas-like API to simultaneously define online and offline feature pipelines, including 
operators like ```transform```, ```filter```, ```join```, ```groupby/aggregate```, ```drop```, etc.
- [Experimental] Perform heavy embedding dot products, query meta-models or simply calculate users age in milliseconds at request time
 using *[On-Demand Features]()*, thanks to Read-Write separate calculation design

## Why use Volga

There are no standalone open-source feature calculation engines/platforms which allow consistent online-offline pipelines without vendor-lock,
setting up complex infra like Spark or Flink simultaneously and/or dependency on proprietary closed-source tech 
(i.e Tecton, FeatureForm, Chalk.ai etc.). Volga fills this spot.

## Quick start

Define data sources

```python
from volga.sources import KafkaSource, MysqlSource

kafka = KafkaSource.get(bootstrap_servers='', username='', password='')
mysql = MysqlSource.get(host='127.0.0.1', port='3306', user='root', password='', database='db')

```

Define input datasets

```python
from volga.datasets import dataset, field, pipeline
from volga.sources import source

@source(mysql.table('users'))
@dataset
class User:
    user_id: str = field(key=True)
    registered_at: datetime.datetime = field(timestamp=True)
    name: str


@source(kafka.topic('orders'), tag='online')
@source(mysql.table('orders'), tag='offline')
@dataset
class Order:
    buyer_id: str = field(key=True)
    product_id: str = field(key=True)
    product_type: str
    purchased_at: datetime.datetime = field(timestamp=True)
    product_price: float

```

Define feature dataset and calculation pipeline

```python
@dataset
class OnSaleUserSpentInfo:
    user_id: str = field(key=True)
    product_id: str = field(key=True)
    timestamp: datetime.datetime = field(timestamp=True)

    avg_spent_7d: float
    avg_spent_1h: float
    num_purchases_1w: int

    @pipeline(inputs=[User, Order])
    def gen(cls, users: Dataset, orders: Dataset):
        on_sale_purchases = orders.filter(lambda df: df['product_type'] == 'ON_SALE')         

        per_user = users.join(on_sale_purchases, left_on=['user_id'], right_on=['buyer_id'])

        return per_user.group_by(keys=['user_id']).aggregate([
            Avg(on='product_price', window= '7d', into='avg_spent_7d'),
            Avg(on='product_price', window= '1h', into='avg_spent_1h'),
            Count(window='1w', into='num_purchases_1w'),
        ])

```

Run offline feature calculation job and get results (i.e. for model training)

```python
from volga import Client

client = Client()

# run batch materialization job
client.materialize_offline(target=OnSaleUserSpentInfo, source_tags={Order: 'offline'})

# query cold offline storage
historical_on_sale_user_spent_df = client.get_offline(targets=[OnSaleUserSpentInfo], start='',  end='')

```

Run online feature calculation job and query real-time updates (i.e. for model inference)

```python
# run real-time job
client.materialize_online(target=OnSaleUserSpentInfo, source_tags={Order: 'online'})

# query hot cache
live_on_sale_user_spent_df = client.get_online_latest(targets=[OnSaleUserSpentInfo], keys=[{'user_id': 1}])

```
