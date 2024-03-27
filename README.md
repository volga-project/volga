## Volga - Data engine for real-time AI/ML

Volga is an open-source, self-serve, scalable data engine tailored for modern real-time AI/ML applications.
It features convenient pandas-like API to define data entities, online/offline pipelines and sources, 
consistent online+offline feature calculation semantics, plugable and configurable hot and cold storage, feature lookups, 
real-time serving and on-demand request-time calculations.
It aims to be a completely standalone and self-serve system removing any heavy-weight dependency on general data processors (Flink, Spark) 
or cloud-based feature platforms (Tecton.ai, Fennel.ai) and can be run on a laptop or on a 1000-node cluster

Features:

- Utilizes *[custom scalable stream processing engine](https://github.com/anovv/volga/blob/master/volga/streaming/runtime/master/test/test_streaming_e2e.py)* using **[Ray Actors](https://docs.ray.io/en/latest/ray-core/actors.html)** for orchestration, 
**[ZeroMQ](https://zeromq.org/)** for messaging and **[Rust](https://www.rust-lang.org/)** for some perf-critical parts (*exeprimental*). 
Kappa architecture - no Flink or Spark
- Built on top of **[Ray](https://github.com/ray-project/ray)** - Easily integrates with Ray ecosystem 
(cluster/job/cloud management, model training/serving, zero-copy data transfers, etc.) as well as your custom ML infrastructure
- Kubernetes ready, no vendor-lock - use **[KubeRay](https://github.com/ray-project/kuberay)** to run multitenant scalable jobs or create your own deployment/scheduling logic in pure Python
- Pure Python, no heavy JVM setups - minimal setup and maintenance efforts in production
- Standalone - launch on your laptop or a cluster with no heavy-weight external dependencies
- Cold + Hot storage model allows for customizable offline storage and caches, fast real-time feature queries
- Easy to use declarative pandas-like API to simultaneously define online and offline feature pipelines, including 
operators like ```transform```, ```filter```, ```join```, ```groupby/aggregate```, ```drop```, etc.
- [Experimental] Perform heavy embedding dot products, query meta-models or simply calculate users age in milliseconds at request time
 using *[On-Demand Features]()*

Volga's API design was inspired by [Fennel](https://fennel.ai/docs/concepts/introduction)

## Why use Volga

There are no self-serve open-source feature calculation engines/platforms which allow consistent online-offline pipelines without vendor-lock,
setting up and managing complex infra like Spark or Flink simultaneously and/or dependency on proprietary closed-source tech 
(i.e Tecton.ai, Fennel.ai, FeatureForm, Chalk.ai etc.). Volga fills this spot.

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
    product_type: str # 'ON_SALE' or 'REGULAR' 
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
        per_user = on_sale_purchases.join(users, right_on=['user_id'], left_on=['buyer_id'])

        return per_user.group_by(keys=['user_id']).aggregate([
            Avg(on='product_price', window= '7d', into='avg_spent_7d'),
            Avg(on='product_price', window= '1h', into='avg_spent_1h'),
            Count(window='1w', into='num_purchases_1w'),
        ])

```

Run offline feature calculation job and get results (i.e. for model training)

```python
from volga import Client
from volga.storage.common.simple_in_memory_actor_storage import SimpleInMemoryActorStorage

storage = SimpleInMemoryActorStorage()
client = Client(hot=storage, cold=storage)

# run batch materialization job
client.materialize_offline(
    target=OnSaleUserSpentInfo, 
    storage=storage,
    source_tags={Order: 'offline'}
)

# query cold offline storage
historical_on_sale_user_spent_df = client.get_offline(targets=[OnSaleUserSpentInfo], start='',  end='')

```

Run online feature calculation job and query real-time updates (i.e. for model inference)

```python
# run real-time job
client.materialize_online(
    target=OnSaleUserSpentInfo, 
    storage=storage,
    source_tags={Order: 'online'}
)

# query hot cache
live_on_sale_user_spent_df = client.get_online_latest(targets=[OnSaleUserSpentInfo], keys=[{'user_id': 1}])

```

## Installation

The project is currently in dev stage and has no published packages.
To run locally/dev locally, clone the repository and in your dev env run:
```
pip install pyproject.toml
```

If on Apple Silicon, when installing Ray you may get an error regarding grpcio on Apple Silicon. To fix, run:
```
pip uninstall grpcio; conda install grpcio
```

To enable graph visualization install GraphViz:
```
brew install graphviz
pip install pygraphviz
```

## Running locally

Since Volga uses Ray's distributed runtime you'll need a running Ray Cluster to run pipelines. The easiest way is to launch
a local one-node cluster:
```
ray start --head
```
Make sure your program's entry point is in the same virtual env where you launched the cluster.
You can run sample e2e tests to see the engine's workflow:
- [test_volga_e2e.py](https://github.com/anovv/volga/blob/master/volga/client/test_volga_e2e.py) - Sample high-level online/offline pipeline
- [test_streaming_e2e.py](https://github.com/anovv/volga/blob/master/volga/streaming/runtime/master/test/test_streaming_e2e.py) - Sample low-level streaming job
```
python test_volga_e2e.py

python test_streaming_e2e.py
```

## Development

Volga is in a proof of concept state hence any feedback is extremely valuable to us. Issues and PRs are always welcome.
