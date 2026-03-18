<h2 align="center">Volga - Real-time data processing for modern AI/ML systems</h2>
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

**[Volga](https://volgaai.substack.com/p/volga-open-source-feature-engine-1)** is a data processing engine aiming to be an alternative to general streaming engines like Flink and Spark as well as AI/ML oriented systems like [Chronon](https://github.com/airbnb/chronon) and [OpenMLDB](https://github.com/4paradigm/OpenMLDB). It is built in Rust using [Apache DataFusion](https://github.com/apache/datafusion) and [Apache Arrow](https://arrow.apache.org/). 

# 🤯 What and why

Modern AI/ML systems (recsys, fraud detection, personalization, search, RAG) rely on features computed from real-time/historical event streams:

- Long continiously sliding window aggregations (`1h`, `7d`, `30d`, `1y`)  
- Point-in-time correct data for batch and inference  
- Consistent online + offline pipeline definitions and computation semantics
- Real-time asynchronous data serving

Today this typically requires combining multiple systems:
- Streaming engine (Flink/Spark)  
- Storage (Redis/S3)  
- Serving layer  
- Feature platforms (Chronon, OpenMLDB, etc.)  

Volga replaces this with a **single engine**:

- Streaming = write path  
- Request = read path  
- Batch = training  


Check the *[blog](https://volgaai.substack.com)*, join *[Slack](https://join.slack.com/t/volga-project/shared_invite/zt-2vqyiyajo-zqgHZKXahQPuqqS~eD~A5w)*.

# 📊 System Comparison

| System          | Execution Model                                   | Storage/State                          | Pipeline Definitions                          | Window Agg Optimization        | Serving                                      |
|-----------------|---------------------------------------------------|--------------------------------|----------------------------------------------|---------------------------------|----------------------------------------------|
| **Volga**       | **native (streaming / batch / request); Rust**          | **native remote (S3 + SlateDB) + local cache**| **SQL + ML funcs** (`top`, `_cate`, etc.)    | ✅ **tiling (native in-state, incremental)**  | **native request-mode + queryable state**     |
| Flink / Spark   | engines (streaming / batch); Java                       | local / checkpointed           | SQL                                          | ❌ none (recompute-heavy)        | external serving layer required              |
| Chronon         | composition (Flink + Spark + Redis); Java/Scala               | local + external KV store      | DSL (feature pipelines)                      | ✅ tiling (via external materialization)  | external serving layer                       |
| OpenMLDB        | DB + Spark (no native streaming engine); C++           | local (memory + disk)          | SQL + ML funcs                                          | ⚠️ limited (no tiling)           | built-in DB serving                          |

# 🌳 Features

### Unified execution
- Streaming, Batch, Request modes via single SQL query
- Same operators and semantics  

### SQL + ML-oriented aggregations
- `top(col, k)`  
- `topn_frequency(col, k)`  
- `top1_ratio(col)`  
- `sum_cate`, `count_cate`, `avg_cate`  
- `_where`, `_cate_where`  

### Long windows via tiling
- Efficient long windows (weeks/months)  
- Incremental tile-based aggregation in-state 
- No recomputation  

### Request-time execution
- Dataflow graph optimization to serve poin-in-time correct data
- Queryable state allows serving data directly
- No Redis / serving layer  
- Built-in read/write compute separation  

### Remote state storage
- Object storage-backed (SlateDB + S3, etc.)  
- Partial state loading  
- Cheap checkpointing  

### High performance
- Rust + Tokio  
- Apache Arrow  
- Apache DataFusion

# 🏠 Architecture

Volga is a **distributed dataflow engine**:

- SQL → dataflow graph (via Apache DataFusion)  
- Operators executed across workers  
- State stored remotely (object storage via SlateDB)  
- Columnar execution (Apache Arrow)  

Core principles:

- Consistent streaming, batch and request execution modes
- SQL first - consistent pipeline definitions across all execution modes
- Compute–storage separation - Remote State Storage
- Read Path-Write Path compute separation - Optimal perf for real-time pipelines
- Queryable state - No external KV stores
- Request-mode execution - Serve same query as batch without external seving layer
- Window Agg Optimizations (Tiling)

# 🚅 Example SQL Pipeline

```sql
SELECT
  u.user_id,

  -- core aggregations
  count(*) OVER w_short AS purchases_1h,
  sum(o.amount) OVER w_long AS spent_30d,

  -- top aggregations
  top(o.product_id, 3) OVER w_long AS top_products_30d,
  topn_frequency(o.product_id, 3) OVER w_long AS top_product_freq_30d,
  top1_ratio(o.product_id) OVER w_rows AS top_product_dominance_last_100,

  -- categorical aggregation
  sum_cate(o.amount, o.product_type) OVER w_long AS spent_per_category,

  -- conditional categorical aggregation
  count_cate_where(o.product_id, o.product_type, o.product_type = 'ON_SALE') OVER w_short AS on_sale_count_1h

WINDOW
  w_short AS (
    PARTITION BY u.user_id 
    ORDER BY o.event_time 
    RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW 
  ),
  w_long AS (
    PARTITION BY u.user_id 
    ORDER BY o.event_time 
    RANGE BETWEEN INTERVAL '30 day' PRECEDING AND CURRENT ROW 
  ),
  w_rows AS (
    PARTITION BY u.user_id 
    ORDER BY o.event_time 
    ROWS BETWEEN 100 PRECEDING AND CURRENT ROW
  )

FROM users u
JOIN orders o
  ON u.user_id = o.buyer_id
```

# 🧩 Running a Pipeline

### Python (JSON spec + client)

```python
import json
from volga.client import Client

pipeline_spec = {
    "name": "user_features",
    "execution_mode": "request",
    "query": """
        SELECT
          user_id,
          count(*) OVER w AS purchases_1h,
          sum(amount) OVER w AS spent_7d,
          topn_frequency(category, 3) OVER w
        WINDOW w as PARTITION BY user_id ORDER BY ts RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW 
        FROM events
    """,
    "sources": [
        {
            "name": "events",
            "type": "kafka",
            "config": {"topic": "events"}
        }
    ]
}

client = Client()
client.create_pipeline(json.dumps(pipeline_spec))
client.start_pipeline("user_features")
```

### Rust (embedded)


```rust
use volga::api::spec::pipeline::PipelineSpec;
use volga::api::pipeline_context::PipelineContext;

let spec = PipelineSpec {
    name: "user_features".to_string(),
    execution_mode: "batch".to_string(),
    query: r#"
        SELECT
          user_id,
          count(*) OVER w AS purchases_1h,
          sum(amount) OVER w AS spent_7d,
          topn_frequency(category, 3) OVER w
        WINDOW w as PARTITION BY user_id ORDER BY ts RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW
        FROM events
    "#.to_string(),
    sources: vec![],
    ..Default::default()
};

let ctx = PipelineContext::try_from(spec)?;
ctx.execute()?;
```

# 🚢 Installation

```bash
git clone https://github.com/volga-project/volga
cd volga
cargo build
```


# 🙇 Running Locally

```bash
cargo test
```

# 🧁 Roadmap

- [ ] Remote state (production-ready)
- [ ] Batch execution mode
- [ ] Join operator
- [ ] Kubernetes deployment
- [ ] Python Client 
- [ ] UI / dashboard
