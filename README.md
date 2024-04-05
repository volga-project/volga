## Volga - Feature Engine for real-time AI/ML

*[Volga](https://medium.com/@andreynovitskiy/volga-open-source-data-engine-for-real-time-ai-part-1-b8d7b16cb9d2)* is an open-source, self-serve, scalable data/feature engine tailored for modern real-time AI/ML applications.
It features convenient pandas-like API to define data entities, online/offline pipelines and sources, 
consistent online+offline feature calculation semantics, plugable and configurable hot and cold storage, feature lookups, 
real-time serving and on-demand request-time calculations.
It aims to be a completely standalone and self-serve system removing any heavy-weight dependency on general data processors (Flink, Spark) 
or cloud-based feature platforms (Tecton.ai, Fennel.ai) and can be run on a laptop or on a 1000-node cluster.

See our *[blog](https://medium.com/@andreynovitskiy/volga-open-source-data-engine-for-real-time-ai-part-1-b8d7b16cb9d2)* for more details on motivation and technical details.
 
Features:

- Utilizes *[custom scalable stream processing engine](https://github.com/anovv/volga/blob/master/volga/streaming/runtime/master/test/test_streaming_e2e.py)* using **[Ray Actors](https://docs.ray.io/en/latest/ray-core/actors.html)** for orchestration, 
**[ZeroMQ](https://zeromq.org/)** for messaging and **[Rust](https://www.rust-lang.org/)** for some perf-critical parts (*exeprimental*). 
Kappa architecture - no Flink or Spark
- Built on top of **[Ray](https://github.com/ray-project/ray)** - Easily integrates with Ray ecosystem 
(cluster/job/cloud management, model training/serving, zero-copy data transfers, etc.) as well as your custom ML infrastructure
- Kubernetes ready, no vendor lock-in - use **[KubeRay](https://github.com/ray-project/kuberay)** to run multitenant scalable jobs or create your own deployment/scheduling logic in pure Python
- Pure Python, no heavy JVM setups - minimal setup and maintenance efforts in production
- Standalone - launch on your laptop or a cluster with no heavy-weight external dependencies
- Cold + Hot storage model allows for customizable offline storage and caches, fast real-time feature queries
- Easy to use declarative pandas-like API to simultaneously define online and offline feature pipelines, including 
operators like ```transform```, ```filter```, ```join```, ```groupby/aggregate```, ```drop```, etc.
- [Experimental] Perform heavy embedding dot products, query meta-models or simply calculate users age in milliseconds at request time
 using *[On-Demand Features]()*

Volga's API design was inspired by [Fennel](https://fennel.ai/docs/concepts/introduction).

## Why use Volga

There are no self-serve open-source feature calculation engines/platforms which allow consistent online-offline pipelines without vendor lock-in,
setting up and managing complex infra like Spark or Flink simultaneously and/or dependency on proprietary closed-source tech 
(i.e Tecton.ai, Fennel.ai, FeatureForm, Chalk.ai etc.). Volga fills this spot. More info *[here](https://medium.com/@andreynovitskiy/volga-open-source-data-engine-for-real-time-ai-part-1-b8d7b16cb9d2)*.


## Quick start

Define data sources

```python
from volga.sources import KafkaSource, MysqlSource

kafka = KafkaSource.get(bootstrap_servers='127.0.0.1:9092', username='root', password='')
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
from volga.pipeline import pipeline

@dataset
class OnSaleUserSpentInfo:
    user_id: str = field(key=True)
    product_id: str = field(key=True)
    timestamp: datetime.datetime = field(timestamp=True)

    avg_spent_7d: float
    avg_spent_1h: float
    num_purchases_1d: int

    @pipeline(inputs=[User, Order])
    def gen(cls, users: Dataset, orders: Dataset):
        on_sale_purchases = orders.filter(lambda df: df['product_type'] == 'ON_SALE')       
        per_user = on_sale_purchases.join(users, right_on=['user_id'], left_on=['buyer_id'])

        return per_user.group_by(keys=['user_id']).aggregate([
            Avg(on='product_price', window= '7d', into='avg_spent_7d'),
            Avg(on='product_price', window= '1h', into='avg_spent_1h'),
            Count(window='1d', into='num_purchases_1d'),
        ])
```
Define ```Client``` and ```Storage``` interfaces for online (```HotStorage```) and offline (```ColdStorage```) features

```python
from volga import Client
from volga.storage.common.simple_in_memory_actor_storage import SimpleInMemoryActorStorage

storage = SimpleInMemoryActorStorage() # default in-memory storage, can be used as both hot and cold
client = Client(hot=storage, cold=storage)
```

Run offline feature calculation job and get results (i.e. for model training)
```python
# run batch materialization job
client.materialize_offline(
    target=OnSaleUserSpentInfo, 
    source_tags={Order: 'offline'},
    paralellism=1
)

# query cold offline storage
historical_on_sale_user_spent_df = client.get_offline_data(
    dataset_name=OnSaleUserSpentInfo.__name__, 
    keys={'user_id': 0}, 
    start=None, end=None # whole dataset
)
```
```
historical_on_sale_user_spent_df
...
  user_id product_id                   timestamp  avg_spent_7d  avg_spent_1h  num_purchases_1d
0       0     prod_0  2024-03-27 11:26:45.514375           100           100                 1
1       0     prod_2  2024-03-27 12:24:45.514375           100           100                 2
2       0     prod_4  2024-03-27 13:22:45.514375           100           100                 3
3       0     prod_6  2024-03-27 14:20:45.514375           100           100                 4
4       0     prod_8  2024-03-27 15:18:45.514375           100           100                 5
```


Run online feature calculation job and query real-time updates (i.e. for model inference)

```python
# run real-time job
client.materialize_online(
    target=OnSaleUserSpentInfo, 
    source_tags={Order: 'online'},
    scaling_config={'Join_1': 4}, # optional parallelism per operator
    _async=True
)

# query hot cache
live_on_sale_user_spent = None
while True:
    res = client.get_online_latest_data(
        dataset_name=OnSaleUserSpentInfo.__name__, 
        keys={'user_id': 0}
    )
    if live_on_sale_user_spent == res:
        # skip same event
        continue
    live_on_sale_user_spent = res
    print(f'[{time.time()}]{res}')
```
```
[1711537166.856853][{'user_id': '0', 'product_id': 'prod_0', 'timestamp': '2024-03-27 14:59:20.124752', 'avg_spent_7d': 100, 'avg_spent_1h': 100, 'num_purchases_1d': 1}]
[1711537167.867083][{'user_id': '0', 'product_id': 'prod_2', 'timestamp': '2024-03-27 15:57:20.124752', 'avg_spent_7d': 100, 'avg_spent_1h': 100, 'num_purchases_1d': 2}]
[1711537169.8647628][{'user_id': '0', 'product_id': 'prod_4', 'timestamp': '2024-03-27 16:55:20.124752', 'avg_spent_7d': 100, 'avg_spent_1h': 100, 'num_purchases_1d': 3}]
...
```

## On-Demand Features (Experimental work in progress)
On-Demand features allow performing stateless transformations at request time, both in online and offline setting.
This can be helpful in cases when transformation is too resource-heavy for streaming or when input data is available 
only at request time (e.g. GPS coordinates, meta-model outputs, etc.)

Define resulting ```@dataset``` with ```@on_demand``` function. On-Demand features can depend on regular datasets (with @pipeline function)
as well as other on-demand datasets - this is configured via ```deps=[Dataset]``` parameter. When materialization is launched (both offline and online), the framework builds 
on-demand task DAG and executes it in parallel on on-demand worker pool (Ray Actors).

```python
from volga.on_demand import on_demand

@dataset
class UserOnSaleTransactionTooBig:
    user_id: str = field(key=True)
    tx_id: str = field(key=True)
    tx_ts: datetime.datetime = field(timestamp=True)

    above_7d_avg: bool
    above_1h_avg: bool

    @on_demand(deps=[OnSaleUserSpentInfo])
    def gen(cls, ts: datetime.datetime, transaction: Dict):
        on_sale_spent_info = OnSaleUserSpentInfo.get(keys=[{'user_id': transaction['user_id'], 'product_id': transaction['product_id']}], ts=ts) # returns Dict-like object
        above_7d_avg = transaction['tx_amount'] > on_sale_spent_info['avg_spent_7d']
        above_1h_avg = transaction['tx_amount'] > on_sale_spent_info['avg_spent_1h']
        
        # output schema should match dataset schema
        return {
            'user_id': transaction['user_id'],
            'tx_id': transaction['tx_id'],
            'tx_ts': ts,
            'above_7d_avg': above_7d_avg,
            'above_1h_avg': above_1h_avg
        }
```

Calculate on-demand transformation, this can be done for both online and offline storages. 
This step will first check availability of all dependant datasets and will fail if not present.

```python
res = client.get_on_demand(
    target=UserOnSaleTransactionTooBig,
    online=True, # False for offline storage source
    start=None, end=None, # datetime range in case of offline request
    inputs=[{
        'user_id': '1',
        'tx_id': 'tx_0',
        'product_id': 'prod_0',
        'tx_amount': 150
    }]
)
...

{'user_id': '1', 'timestamp': '2024-03-27 14:59:20.124752', 'tx_id': 'tx_0', 'above_7d_avg': 'false', 'above_1h_avg': 'true'}
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

Volga is in a proof of concept state and requires some key features to be prod-ready (checkpointing and state backend, 
watermarks, etc.), you can see the backlog here

**[BACKLOG](https://github.com/users/anovv/projects/1/views/1)**

Any feedback is extremely valuable, issues and PRs are always welcome.
