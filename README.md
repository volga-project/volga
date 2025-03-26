<h2 align="center">Volga - Real-time data processing in Python for modern AI/ML systems</h2>
<p align="center">
  <img src="https://github.com/volga-project/volga/blob/master/.github/logo_white_bckgr_50_pct.png?raw=true" width="150" height="150">
</p>

<div align="center">
  
  <a href="">![GitHub License](https://img.shields.io/github/license/volga-project/volga)</a>
  <a href="">![Static Badge](https://img.shields.io/badge/Volga-visit_blog-blue?link=https%3A%2F%2Fvolgaai.substack.com%2F)</a>
  <a href="">![GitHub Created At](https://img.shields.io/github/created-at/volga-project/volga)</a>
  <a href="">![GitHub last commit](https://img.shields.io/github/last-commit/volga-project/volga)</a>
  <a href="">![GitHub top language](https://img.shields.io/github/languages/top/volga-project/volga)</a>
  <a href="">![GitHub Issues or Pull Requests](https://img.shields.io/github/issues-raw/volga-project/volga)</a>
  
</div>

Volga is a general purpose real-time data processing engine aiming to be a fully functional Python-native Flink/Spark Streaming alternative with extended functionality to support modern real-time AI/ML systems without dependency on 3rd party feature platforms.

# 📖 Table of contents
* [📖 Table of contents](#-table-of-contents)
* [🤯 What and why](#-what-and-why)
* [🏠 Architecture](#-architecture)
* [🌳 Features](#-features)
* [🚅 Quick Start](#-quick-start)
  * [Entity API](#entity-api-example)
  * [DataStream API](#datastream-api-example)
* [🚢 Installation](#-installation)
* [🙇 Running Locally](#-running-locally)
* [🧁 Roadmap](#-roadmap)

# 🤯 What and why

**[Volga](https://volgaai.substack.com/p/volga-open-source-feature-engine-1)** is a **general purpose real-time data processing engine tailored for modern AI/ML applications**. It aims to be a fully functional Python-native Flink/Spark Streaming alternative with extended functionality to execute environment-agnostic event-time/request-time computations (common in real-time AI/ML workloads - fraud detection, recommender systems, search personalization, dynamic pricing, credit risk, ETA calculation, personalized real-time context-aware RAG, etc.).

Volga is designed to be a backbone for your custom real-time AI/ML feature platforms or general data pipelines without relying on heterogenous data processors like Flink/Spark/custom data processing layers (e.g. Chronon) or third party data/feature platforms (e.g. Tecton.ai, Fennel.ai, Chalk.ai).

Check the *[blog](https://volgaai.substack.com)*, join *[Slack](https://join.slack.com/t/volga-project/shared_invite/zt-2vqyiyajo-zqgHZKXahQPuqqS~eD~A5w)*, see *[v1.0 Relaese Roadmap](https://github.com/volga-project/volga/issues/69)*.

# 🏠 Architecture

Volga provides a ***Python-native runtime*** in conjunction with ***Rust*** for performance, runs on ***[Ray](https://github.com/ray-project/ray)***, uses a ***hybrid push(streaming) + pull(on-demand) architecture*** to run arbitrary request-time/event-time computation DAGs, features convenient Pandas-like **Entity API** to define data entities and online/offline feature pipelines and on-demand features as well as general purpose **DataStream API** for more general data processing cases, 
consistent online+offline feature calculation semantics, configurable storage, real-time data serving and request-time compute. It can run on a laptop or a distributed cluster.

![alt text](https://github.com/volga-project/volga/blob/master/.github/volga_arch.png?raw=true)

# 🌳 Features

- *Hybrid push+pull architecture*: **custom Streaming Engine (the *Push part*)** and **On-Demand Compute Layer (the *Pull part*)** to execute environment-agnostic computation DAGs.  
- **[Streaming Engine](https://volgaai.substack.com/p/volga-streaming-engine-and-networking-1)** built with *Ray Actors*, *ZeroMQ*, *Rust* and *PyO3*. Python-native Flink alternative with flexible ***DataStream API*** to build custom streaming pipelines that **[scale to millitons of messages per second with latency in milliseconds](https://volgaai.substack.com/p/volga-streaming-engine-and-networking-3)**.
- **[On-Demand Compute Layer](https://volgaai.substack.com/p/volga-open-source-feature-engine-2)** to perform arbitrary DAGs of request time/inference time calculations in sync with streaming engine (real-time feature serving, request-time heavy embedding dot products, meta-models query/feature enrichment or simply calculating users age in milliseconds).
- ***Entity API*** to build standardized data models with compile-time schema validation, Pandas-like operators like ```transform```, ```filter```, ```join```, ```groupby/aggregate```, ```drop```, etc. to build modular AI/ML features with consistent online/offline semantics.
- Built on top of **[Ray](https://github.com/ray-project/ray)** - Easily integrates with Ray ecosystem, runs on Kubernetes and local machines, provides a homogeneous platform with no heavy dependencies on multiple JVM-based systems.
- Highly customizable data connectors to read/write data from/to any third party system.

<p align="center">
  <img src="https://github.com/volga-project/volga/blob/master/.github/cluster_combined_crop_500.png?raw=true">
</p>

# 🚅 Quick Start

Volga provides two sets of APIs to build and run data pipelines: high-level **Entity API** to build environment-agnostic computation DAGs (commonly used in real-time AI/ML feature pipelines) and low-level Flink-like **DataStream API** for general streaming/batch pipelines. 
In a nutshell, Entity API is built on top of DataStream API (for streaming/batch features calculated on streaming engine) and also includes interface for On-Demand Compute Layer (for features calculated at request time) and consistent data models/abstractions to stitch both systems together.

## Entity API example

Entity API distinguishes between two types of features: ```@pipeline``` (online/streaming + offline/batch which are executed on streaming engine) and ```@on_demand``` (request time execution on On-Demand Compute Layer), both types operate on ```@entity``` annotated data models.

- Use ```@entity``` to decorate data models. Note ```key``` and ```timestamp```: keys are used to group/join/lookup features, timestamps are used for point-in-time correctness, range queries and lookups by on-demand features. Entities should also have logically matching schemas - this is enforced at compile-time which is extremely useful for data consistency. 

```python
from volga.api.entity import Entity, entity, field

# User, Order and OnSaleUserSpentInfo are used by @pipeline features
@entity
class User:
    user_id: str = field(key=True)
    registered_at: datetime.datetime = field(timestamp=True)
    name: str

@entity
class Order:
    buyer_id: str = field(key=True)
    product_id: str = field(key=True)
    product_type: str
    purchased_at: datetime.datetime = field(timestamp=True)
    product_price: float

@entity
class OnSaleUserSpentInfo:
    user_id: str = field(key=True)
    timestamp: datetime.datetime = field(timestamp=True)
    avg_spent_7d: float
    num_purchases_1h: int

# UserStats is an entity returned to user as a resutl of corresponding @on_demand feature
@entity
class UserStats:
    user_id: str = field(key=True)
    timestamp: datetime.datetime = field(timestamp=True)
    total_spent: float
    purchase_count: int
```

- Define pipeline features (streaming/batch) using ```@source``` and ```@pipeline``` decorators. We explicilty specify dependant features in ```dependencies``` which our engine automatically maps to function arguments at job graph compile time. Note that pipelines can only depend on other pipelines

```python
from volga.api.pipeline import pipeline
from volga.api.source import Connector, MockOnlineConnector, source, MockOfflineConnector

users = [...] # sample User entities
orders = [...] # sample Order entities

@source(User)
def user_source() -> Connector:
    return MockOfflineConnector.with_items([user.__dict__ for user in users])

@source(Order)
def order_source(online: bool = True) -> Connector: # this will generate appropriate connector based on param we pass during execution
    if online:
        return MockOnlineConnector.with_periodic_items([order.__dict__ for order in orders], period_s=purchase_event_delays_s)
    else:
        return MockOfflineConnector.with_items([order.__dict__ for order in orders])

@pipeline(dependencies=['user_source', 'order_source'], output=OnSaleUserSpentInfo)
def user_spent_pipeline(users: Entity, orders: Entity) -> Entity:
    on_sale_purchases = orders.filter(lambda x: x['product_type'] == 'ON_SALE')
    per_user = on_sale_purchases.join(
        users, 
        left_on=['buyer_id'], 
        right_on=['user_id'],
        how='left'
    )
    return per_user.group_by(keys=['buyer_id']).aggregate([
        Avg(on='product_price', window='7d', into='avg_spent_7d'),
        Count(window='1h', into='num_purchases_1h'),
    ]).rename(columns={
        'purchased_at': 'timestamp',
        'buyer_id': 'user_id'
    })
```

- Run batch (offline) materialization. This is needed if you want offline features for model training.

```python
from volga.client.client import Client
from volga.api.feature import FeatureRepository

client = Client()
pipeline_connector = InMemoryActorPipelineDataConnector(batch=False) # store data in-memory, can be any other user-defined connector, e.g. Redis/Cassandra/S3

# Note that offline materialization only works for pipeline features at the moment, so offline data points you get will match event time, not request time
client.materialize(
    features=[FeatureRepository.get_feature('user_spent_pipeline')],
    pipeline_data_connector=InMemoryActorPipelineDataConnector(batch=False),
    _async=False,
    params={'global': {'online': False}}
)

# Get results from storage. This will be specific to what db you use

keys = [{'user_id': user.user_id} for user in users]

# we user in-memory actor
offline_res_raw = ray.get(cache_actor.get_range.remote(feature_name='user_spent_pipeline', keys=keys, start=None, end=None, with_timestamps=False))
offline_res_flattened = [item for items in offline_res_raw for item in items]
offline_res_flattened.sort(key=lambda x: x['timestamp'])
offline_df = pd.DataFrame(offline_res_flattened)
pprint(offline_df)

...

    user_id                  timestamp  avg_spent_7d  num_purchases_1h
0         0 2025-03-22 13:54:43.335568         100.0                 1
1         1 2025-03-22 13:54:44.335568         100.0                 1
2         2 2025-03-22 13:54:45.335568         100.0                 1
3         3 2025-03-22 13:54:46.335568         100.0                 1
4         4 2025-03-22 13:54:47.335568         100.0                 1
..      ...                        ...           ...               ...
796      96 2025-03-22 14:07:59.335568         100.0                 8
797      97 2025-03-22 14:08:00.335568         100.0                 8
798      98 2025-03-22 14:08:01.335568         100.0                 8
799      99 2025-03-22 14:08:02.335568         100.0                 8
800       0 2025-03-22 14:08:03.335568         100.0                 9
```

- For online inference, define on-demand features using ```@on_demand``` - this will be executed at request time. Unlike ```@pipeline```, ```@on_demand``` can depend on both pipelines and on-demands. If we just want to serve pipeline results, we can skip this.

```python
from volga.api.on_demand import on_demand

@on_demand(dependencies=[(
  'user_spent_pipeline', # name of dependency, matches positional argument in function
  'latest' # name of the query defined in OnDemandDataConnector - how we access dependant data (e.g. latest, last_n, average, etc.).
)])
def user_stats(spent_info: OnSaleUserSpentInfo) -> UserStats:
    # logic to execute at request time
    return UserStats(
        user_id=spent_info.user_id,
        timestamp=spent_info.timestamp,
        total_spent=spent_info.avg_spent_7d * spent_info.num_purchases_1h,
        purchase_count=spent_info.num_purchases_1h
    )
```

- To be able to request results in real-time, run streaming (online) materialization.
  
```python

client.materialize(
    features=[FeatureRepository.get_feature('user_spent_pipeline')],
    pipeline_data_connector=pipeline_connector,
    job_config=DEFAULT_STREAMING_JOB_CONFIG,
    scaling_config={},
    _async=True,
    params={'global': {'online': True}}
)
```

- In a separate thread/process/node, request results in real-time - simulate inference feature time requests

```python
# Start OnDemand coordinator
coordinator = create_on_demand_coordinator(OnDemandConfig(
    num_servers_per_node=1,
    server_port=DEFAULT_ON_DEMAND_SERVER_PORT,
    data_connector=OnDemandDataConnectorConfig(
        connector_class=InMemoryActorOnDemandDataConnector, # defines how OnDemand layer reads pipeline data, maps custom queries to query arguments from OnDemandRequest
        connector_args={}
    )
))
await coordinator.start.remote() # create OnDemand workers on the cluster

# register features to execute
await coordinator.register_features.remote(FeatureRepository.get_all_features())

# query results
client = OnDemandClient(DEFAULT_ON_DEMAND_CLIENT_URL)
user_ids = [...] # user ids you want to query

while True:
    request = OnDemandRequest(
        target_features=['user_stats'],
        feature_keys={
            'user_stats': [
                {'user_id': user_id} 
                for user_id in user_ids
            ]
        },
        query_args={
            'user_stats': {}, # empty for 'latest', can be time range if we have 'last_n' query or any other query/params configuration defined in data connector
        }
    )
    
    response = await self.client.request(request)

    for user_id, user_stats_raw in zip(user_ids, response.results['user_stats']):
        user_stats = UserStats(**user_stats_raw[0])
        pprint(f'New feature: {user_stats.__dict__}')

...

("New feature: {'user_id': '98', 'timestamp': '2025-03-22T10:04:54.685096', "
 "'total_spent': 400.0, 'purchase_count': 4}")
("New feature: {'user_id': '99', 'timestamp': '2025-03-22T10:04:55.685096', "
 "'total_spent': 400.0, 'purchase_count': 4}")
("New feature: {'user_id': '0', 'timestamp': '2025-03-22T10:04:56.685096', "
 "'total_spent': 500.0, 'purchase_count': 5}")
("New feature: {'user_id': '1', 'timestamp': '2025-03-22T10:04:57.685096', "
 "'total_spent': 500.0, 'purchase_count': 5}")
("New feature: {'user_id': '2', 'timestamp': '2025-03-22T10:04:58.685096', "
 "'total_spent': 500.0, 'purchase_count': 5}")

```

## DataStream API example

If we want to have more control over streaming engine and build any other custom pipeline, Volga exposes general purpose Flink-like DataStream API to define consistent streaming/batch pipelines.

```python
from volga.streaming.api.context.streaming_context import StreamingContext
from volga.streaming.api.context.streaming_context import StreamingContext
from volga.streaming.api.function.function import SinkToCacheDictFunction
from volga.streaming.api.stream.sink_cache_actor import SinkCacheActor
from volga.streaming.runtime.sources.wordcount.timed_source import WordCountSource

# simple word count

sink_cache = SinkCacheActor.remote() # simple actor with list and dict to store data

ctx = StreamingContext(job_config=job_config)

source = WordCountSource(
    streaming_context=ctx,
    dictionary=dictionary,
    run_for_s=run_for_s
) # round robins words from dictionary to each peer worker based on parallelism

source.set_parallelism(2) # 2 workers/tasks per operator

s = source.map(lambda wrd: (wrd, 1)) \
    .key_by(lambda e: e[0]) \
    .reduce(lambda old_value, new_value: (old_value[0], old_value[1] + new_value[1]))

s.sink(SinkToCacheDictFunction(sink_cache, key_value_extractor=(lambda e: (e[0], e[1]))))

ctx.execute()
```

# 🚢 Installation

The project is currently in [dev stage](https://github.com/volga-project/volga/issues/69) and has no published packages/binaries yet.
To run locally/dev locally, clone the repository and in your dev env run:
```
pip install .
```

After that make sure to compile rust binaries and build Python binding using PyO3's ```maturin``` :
```
cd rust
maturin develop
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

To develop locally and have Ray pick up local changes, you need to uninstall local volga package and append 
PYTHONPATH env var with a path to your local volga project folder:
```
pip uninstall volga
export PYTHONPATH=/path/to/local/volga/folder:$PYTHONPATH # you need to set it in each new shell/window. Alternatively, use env vars setting in you virtual env maanger
```


# 🙇 Running locally

Since Volga uses Ray's distributed runtime you'll need a running Ray Cluster to run pipelines. The easiest way is to launch
a local one-node cluster:
```
ray start --head
```
Make sure your program's entry point is in the same virtual env where you launched the cluster.
You can run sample e2e tests to see the engine's workflow:
- [test_volga_e2e.py](https://github.com/volga-project/volga/blob/master/volga_tests/test_volga_e2e.py) - Sample high-level on-demand/pipeline workflow
- [test_streaming_e2e.py](https://github.com/volga-project/volga/blob/master/volga_tests/test_streaming_wordcount.py) - Sample low-level streaming job
```
python test_volga_e2e.py

python test_streaming_e2e.py
```

The development is done with Python 3.10.8 and Ray 2.22.0, in case of any import/installation related errors, please try rolling
your dev env back to these versions.

# 🧁 Roadmap

Volga is in active development and requires a number of features to get to prod-ready release (mostly around fault-tolerance: streaming engine checkpointing and state backend, 
watermarks, on-demand worker health, etc.), you can see the release roadmap here:

**[Volga v1.0 Relase Roadmap](https://github.com/volga-project/volga/issues/69)**

Any feedback is extremely valuable, issues and PRs are always welcome.
