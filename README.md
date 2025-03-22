<h2 align="center">Volga - Data Processing/Feature Calculation Engine for Real-Time AI/ML</h2>
<p align="center">
  <img src="https://github.com/volga-project/volga/blob/master/.github/logo_white_bckgr_100.png?raw=true" width="100" height="100">
</p>

# What and why

**[Volga](https://volgaai.substack.com/p/volga-open-source-feature-engine-1)** is a **real-time data processing engine for real-time AI/ML**. It aims to be a fully functional Python-native Flink/Spark Streaming alternative with extended functionality to execute environment-agnostic event-time/request-time computations (common in real-time AI/ML workloads - feature engineering, RAG, etc.).

Volga is designed to be a backbone for your custom real-time AI/ML feature platforms or general data pipelines without relying on heterogenous data processors like Flink/Spark/custom data processing layers (e.g. Chronon) or third party data/feature platforms (e.g. Tecton.ai, Fennel.ai, Chalk.ai).

Subscribe to our *[blog](https://volgaai.substack.com)*, join our *[Slack](https://join.slack.com/t/volga-project/shared_invite/zt-2vqyiyajo-zqgHZKXahQPuqqS~eD~A5w)*.

Volga provides a ***Python-native runtime*** in conjunction with ***Rust*** for performance, runs on ***[Ray](https://github.com/ray-project/ray)***, uses a ***hybrid push(streaming) + pull(on-demand) architecture*** to run arbitrary request-time/event-time computation DAGs, features convenient Pandas-like **Entity API** to define data entities and online/offline pipelines, 
consistent online+offline feature calculation semantics, configurable storage, real-time data serving and request-time calculations. It can run on a laptop or a distributed cluster.

# Features
![alt text](https://github.com/volga-project/volga/blob/master/.github/volga_arch.png?raw=true)

- *Hybrid push+pull architecture*: **custom Streaming Engine (the *Push part*)** and **On-Demand Compute Layer (the *Pull part*)** to execute environment-agnostic computation DAGs.  
- General purpose *[Streaming Engine](https://volgaai.substack.com/p/volga-streaming-engine-and-networking-1)* built with **[Ray Actors](https://docs.ray.io/en/latest/ray-core/actors.html)** for orchestration, 
**[ZeroMQ](https://zeromq.org/)** for messaging, **[Rust](https://www.rust-lang.org/)** and **[PyO3](https://github.com/PyO3/pyo3)** for performance and *Python-native runtime*. Python-native Flink altenative with flexible DataSet API to build custom streaming pipelines.
- **[On-Demand Compute Layer](https://volgaai.substack.com/p/volga-open-source-feature-engine-2)** to perform request time/inference time calculations in sync with streaming engine (real-time feature serving, request-time heavy embedding dot products, meta-models query/feature enrichment or simply calculating users age in milliseconds).
- Built on top of **[Ray](https://github.com/ray-project/ray)** - Easily integrates with Ray ecosystem 
(cluster/job/cloud management, model training/serving, zero-copy data transfers, etc.) as well as custom ML infrastructure.
- Kubernetes ready - use **[KubeRay](https://github.com/ray-project/kuberay)** to run multitenant scalable jobs or create your own deployment/scheduling logic in pure Python.
- Python-native runtime, no heavy JVM setups - easily import all of the Python ecosystem to your pipeline, minimal setup and maintenance efforts.
- Standalone - runs on a laptop or a distributed cluster.
- Standartized Entity data models with compile-time schema validation and customizable data connectors.
- Declarative Pandas-like API to define consistent online+offline feature pipelines, including 
operators like ```transform```, ```filter```, ```join```, ```groupby/aggregate```, ```drop```, etc.

# Performance

Volga's Streaming Engine scales to millions of events per second with millisecond-scale latency. See more *[here](https://volgaai.substack.com/p/volga-streaming-engine-and-networking-3)*

![alt text](https://github.com/volga-project/volga/blob/master/.github/cluster_combined.png)


# Quick start

Volga provides two sets of APIs to build and run data pipelines: high-level Entity API to build environment-agnostic computation DAGs (commonly used in real-time AI/ML feature pipelines) and low-level Flink-like DataStream API for general streaming/batch pipelines.

## Entity API example

- Define entities

```python
from volga.api.entity import Entity, entity, field

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

@entity
class UserStats:
    user_id: str = field(key=True)
    timestamp: datetime.datetime = field(timestamp=True)
    total_spent: float
    purchase_count: int
```

- Define streaming/batch pipelines

```python
from volga.api.pipeline import pipeline
from volga.api.source import Connector, MockOnlineConnector, source, MockOfflineConnector

@source(User)
def user_source() -> Connector:
    return MockOfflineConnector.with_items([user.__dict__ for user in users])

@source(Order)
def order_source() -> Connector:
    return MockOnlineConnector.with_periodic_items([order.__dict__ for order in orders], period_s=purchase_event_delays_s)

@pipeline(dependencies=['user_source', 'order_source'], output=OnSaleUserSpentInfo) # pipelines can only depend on other pipelines
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

- Define on-demand transforms

```python
from volga.api.on_demand import on_demand

@on_demand(dependencies=[(
  'user_spent_pipeline', # name of dependency, matches positional argument in function
  'latest' # name of the query defined in OnDemandDataConnector - how we access dependant data (e.g. latest, last_n, average, etc.).
)]) # can depend on multiple pipelines and other on-demand transforms to build custom DAG.
def user_stats(spent_info: OnSaleUserSpentInfo) -> UserStats:
    # logic to execute at request time
    return UserStats(
        user_id=spent_info.user_id,
        timestamp=spent_info.timestamp,
        total_spent=spent_info.avg_spent_7d * spent_info.num_purchases_1h,
        purchase_count=spent_info.num_purchases_1h
    )
```


- Run streaming/batch materialization
  
```python
from volga.client.client import Client
from volga.api.feature import FeatureRepository

client = Client()
pipeline_connector = InMemoryActorPipelineDataConnector(batch=False) # store data in-memory, can be any other user-defined connector, e.g. Redis/Cassandra/S3

client.materialize(
    features=[FeatureRepository.get_feature('user_spent_pipeline')],
    pipeline_data_connector=pipeline_connector,
    job_config=DEFAULT_STREAMING_JOB_CONFIG
    scaling_config={}
    _async=True
)
```

- In a separate thread/process/node, request results

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

```

- See result

```

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

# Installation

The project is currently in dev stage and has no published packages/binaries.
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


# Running locally

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

# Development

Volga is in a active development state and requires some key features to be prod-ready (checkpointing and state backend, 
watermarks, etc.), you can see the backlog here

**[BACKLOG](https://github.com/orgs/volga-project/projects/2/views/1)**

Any feedback is extremely valuable, issues and PRs are always welcome.
