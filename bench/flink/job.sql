-- Continuous 10s RANGE window. Three statements: source, sink, insert.
-- rows-per-second is the total rate (Flink 1.20 splits it across source subtasks).
-- key_id is uniform in [0, 15]. ts is generation time (max-past 0).

CREATE TABLE datagen_source (
  ts TIMESTAMP(3),
  key_id INT,
  val DOUBLE,
  WATERMARK FOR ts AS ts - INTERVAL '0' SECOND
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '200',
  'fields.key_id.kind' = 'random',
  'fields.key_id.min' = '0',
  'fields.key_id.max' = '15',
  'fields.val.kind' = 'random',
  'fields.val.min' = '1',
  'fields.val.max' = '2',
  'fields.ts.max-past' = '0s'
);

CREATE TABLE blackhole_sink (
  ts TIMESTAMP(3),
  key_id INT,
  val DOUBLE,
  sum_val DOUBLE,
  cnt_val BIGINT,
  avg_val DOUBLE,
  min_val DOUBLE,
  max_val DOUBLE
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO blackhole_sink
SELECT
  ts,
  key_id,
  val,
  SUM(val) OVER w AS sum_val,
  COUNT(val) OVER w AS cnt_val,
  AVG(val) OVER w AS avg_val,
  MIN(val) OVER w AS min_val,
  MAX(val) OVER w AS max_val
FROM datagen_source
WINDOW w AS (
  PARTITION BY key_id
  ORDER BY ts
  RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW
);
