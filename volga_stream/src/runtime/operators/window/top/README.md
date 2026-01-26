# Top Aggregates

This document describes the `top*` family of window aggregates implemented in `window/top`.

## Common Output Format

- Most `top*` functions return `Utf8` strings.
- For key/metric pairs, output is a CSV of `key:metric` (e.g. `c:4,b:3`).
- `top(value, n)` returns a CSV of values (duplicates preserved).
- `top1_ratio` returns a `Float64` scalar, not a string.

## Non-Cate Top Aggregates

### `top(value, n)`

**Syntax**
```
top(value_expr, n_expr) OVER w
```

**Description**
Returns the top `n` values ordered by value (descending). Duplicates are preserved.

**Example**
```
SELECT top(value, 3) OVER w AS top_val
```

**Example Input (columns: timestamp, value)**
```
(1000, 1.0)
(2000, 2.0)
(3000, 4.0)
(4000, 4.0)
(5000, 3.0)
```

**Example Output**
```
Utf8("4,4,3")
```

**Impl Notes**
Uses `TopValueAccumulator` and `TopKMap` ordered by key (value) with counts per distinct value.

---

### `topn_frequency(value, n)`

**Syntax**
```
topn_frequency(value_expr, n_expr) OVER w
```

**Description**
Returns the top `n` values ordered by frequency (descending). Output is a CSV of values only.

**Example**
```
SELECT topn_frequency(value, 2) OVER w AS top_val
```

**Example Input (columns: timestamp, value)**
```
(1000, 1.0)
(2000, 1.0)
(3000, 2.0)
(4000, 2.0)
(5000, 2.0)
(6000, 3.0)
```

**Example Output**
```
Utf8("2,1")
```

**Impl Notes**
Uses `FrequencyTopKAccumulator` in `TopN` mode. Maintains counts per value and a `TopKMap`
ordered by metric (count).

---

### `top1_ratio(value)`

**Syntax**
```
top1_ratio(value_expr) OVER w
```

**Description**
Returns the ratio of the most frequent value to total non-null rows. Output is `Float64`.

**Example**
```
SELECT top1_ratio(value) OVER w AS ratio
```

**Example Input (columns: timestamp, value)**
```
(1000, 1.0)
(2000, 1.0)
(3000, 2.0)
(4000, 2.0)
```

**Example Output**
```
Float64(0.5)
```

**Impl Notes**
Uses `FrequencyTopKAccumulator` in `Top1Ratio` mode. Tracks total non-null rows and top-1 count.

## Cate Top Aggregates

### `top_n_key_{sum|avg|min|max|count}_cate_where(value, cond, cate, n)`

**Syntax**
```
top_n_key_sum_cate_where(value_expr, cond_expr, cate_expr, n_expr) OVER w
```

**Description**
Per category key, compute the aggregate (sum/avg/min/max/count) over rows where `cond` is true.
Order by key (descending) and return the top `n` keys with their aggregate values.

**Example**
```
SELECT top_n_key_sum_cate_where(value, value > 0, partition_key, 2) OVER w AS top_val
```

**Example Input (columns: timestamp, value, cate)**
```
(1000, 1.0, "a")
(2000, 3.0, "b")
(3000, 5.0, "b")
(4000, 2.0, "c")
(5000, 4.0, "c")
```

**Example Output**
```
Utf8("c:6,b:8")
```

**Impl Notes**
Uses `GroupedAggTopKAccumulator` with `TopKOrder::KeyDesc`, one DataFusion accumulator per key.

---

### `top_n_value_{sum|avg|min|max|count}_cate_where(value, cond, cate, n)`

**Syntax**
```
top_n_value_sum_cate_where(value_expr, cond_expr, cate_expr, n_expr) OVER w
```

**Description**
Per category key, compute the aggregate (sum/avg/min/max/count) over rows where `cond` is true.
Order by aggregate value (descending) and return the top `n` keys with their aggregate values.

**Example**
```
SELECT top_n_value_sum_cate_where(value, value > 0, partition_key, 2) OVER w AS top_val
```

**Example Input (columns: timestamp, value, cate)**
```
(1000, 1.0, "a")
(2000, 3.0, "b")
(3000, 5.0, "b")
(4000, 2.0, "c")
(5000, 4.0, "c")
```

**Example Output**
```
Utf8("b:8,c:6")
```

**Impl Notes**
Uses `GroupedAggTopKAccumulator` with `TopKOrder::MetricDesc`.

---

### `top_n_key_ratio_cate(value, cond, cate, n)`

**Syntax**
```
top_n_key_ratio_cate(value_expr, cond_expr, cate_expr, n_expr) OVER w
```

**Description**
Per category key, compute `matched / total` where:
- `total` = count of rows with non-null value and category
- `matched` = count of rows where `cond` is true (and value/category are non-null)

Order by key (descending) and return top `n` keys with their ratios.

**Example**
```
SELECT top_n_key_ratio_cate(value, value > 1, partition_key, 2) OVER w AS top_val
```

**Example Input (columns: timestamp, value, cate)**
```
(1000, 1.0, "c")
(2000, 2.0, "b")
(3000, 3.0, "b")
```

**Example Output**
```
Utf8("c:0,b:1")
```

**Impl Notes**
Uses `RatioTopKAccumulator` with `TopKOrder::KeyDesc`.

---

### `top_n_value_ratio_cate(value, cond, cate, n)`

**Syntax**
```
top_n_value_ratio_cate(value_expr, cond_expr, cate_expr, n_expr) OVER w
```

**Description**
Same ratio as `top_n_key_ratio_cate`, but ordered by ratio (descending), and then by key.

**Example**
```
SELECT top_n_value_ratio_cate(value, value > 1, partition_key, 2) OVER w AS top_val
```

**Example Input (columns: timestamp, value, cate)**
```
(1000, 1.0, "c")
(2000, 2.0, "b")
(3000, 3.0, "b")
```

**Example Output**
```
Utf8("b:1,c:0")
```

**Impl Notes**
Uses `RatioTopKAccumulator` with `TopKOrder::MetricDesc`.

## Shared Implementation Notes

- `TopKMap` keeps a `HashMap` as source of truth and a `BinaryHeap` as a cache for ordering.
- Lazy invalidation removes stale heap entries during evaluation (not during update/retract).
- All accumulators are retractable and follow the `update_batch -> retract_batch -> evaluate` cycle.
