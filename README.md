# Distributed Data Engines

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Java 8+](https://img.shields.io/badge/Java-8%2B-blue.svg)](https://www.oracle.com/java/)
[![Hadoop 2.8+](https://img.shields.io/badge/Hadoop-2.8%2B-brightgreen.svg)](https://hadoop.apache.org/)
[![Spark 3.5+](https://img.shields.io/badge/Spark-3.5%2B-orange.svg)](https://spark.apache.org/)

Systems implementations, algorithmic optimization patterns, and empirical performance evaluations across large-scale distributed data engines:
1. **Hadoop MapReduce Algorithmic Patterns (Java):** Order Inversion pattern for $O(1)$ streaming reducer memory, Pairs vs. Stripes co-occurrence models, and Two-Pass Correlation pipelines with HDFS Distributed Cache preloading.
2. **Apache Spark Telemetry & Shuffle Optimization (PySpark):** Profiling shuffle boundaries, `groupByKey()` vs. `reduceByKey()` memory execution plans, and large-scale unstructured HTTP log analytics.
3. **Cloud Virtualization & Subsystem Benchmarking (AWS EC2):** Empirical profiling of SMT hyperthread contention vs. dedicated silicon cores, multi-channel DDR4 memory bus saturation, and intra-VPC vs. cross-region WAN transit latency.

---

## Architecture Overview

```
distributed-data-engines/
├── mapreduce-patterns/          # Production MapReduce design patterns in Java
│   ├── pom.xml
│   ├── data/                    # Sample corpus (1400-8.txt)
│   └── src/main/java/io/github/frieddeli/mapreduce/
│       ├── BigramFrequencyPairs.java   # Order Inversion Pattern (O(1) memory)
│       ├── BigramFrequencyStripes.java # In-Mapper Combining (associative map)
│       ├── CORPairs.java               # Two-pass correlation via HDFS cache
│       ├── CORStripes.java             # Two-pass correlation via associative stripes
│       ├── PairOfStrings.java          # Custom WritableComparable composite key
│       └── HashMapStringIntWritable.java # Custom Writable associative map
├── spark-telemetry/             # PySpark RDD analytics & shuffle optimization
│   ├── requirements.txt
│   ├── word_count.py            # RDD DAG analysis: reduceByKey vs groupByKey
│   └── log_analysis.py          # NASA Common Log Format ETL & 404 diagnostics
└── cloud-benchmarks/            # AWS EC2 hardware microbenchmarks & analysis
    └── README.md                # SMT scaling, 6-channel DDR4, & WAN transit metrics
```

---

## 1. MapReduce Design Patterns (`mapreduce-patterns/`)

### The Pairs vs. Stripes Co-occurrence Dilemma
Computing bigram relative frequency:
$$P(B \mid A) = \frac{\text{Count}(A, B)}{\sum_{B'} \text{Count}(A, B')} = \frac{\text{Count}(A, B)}{\text{Marginal}(A)}$$

- **Pairs (`BigramFrequencyPairs.java`):** Emits `(A, B) -> 1`. Guarantees minimal memory footprint in the Mapper ($O(1)$ state per pair), but generates $O(N)$ intermediate key-value records that saturate cluster network shuffle bandwidth.
- **Stripes (`BigramFrequencyStripes.java`):** Maintains in-memory associative stripes `Text(A) -> HashMapStringIntWritable(B -> count)`. Minimizes network shuffle traffic, but incurs high JVM heap pressure ($O(V)$ vocabulary size per stripe).

### The Order Inversion Design Pattern
Calculating $P(B \mid A)$ in the Pairs approach normally requires the Reducer to buffer all pairs for word $A$ in memory to compute the marginal sum before dividing. To eliminate unbounded buffer allocation:

1. **Mapper emits sentinel pair:** Emits `(A, "*") -> 1` before emitting each bigram `(A, B) -> 1`.
2. **Custom Partitioner:** Hashes strictly on the left word $A$:
   ```java
   public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks) {
       return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
   }
   ```
3. **Deterministic ASCII Sorting:** Because ASCII `*` (42) sorts strictly before alphanumeric characters, the Reducer always receives `(A, "*")` first. It captures `marginal = sum` in a scalar primitive and processes subsequent pairs in a single streaming pass—achieving **$O(1)$ Reducer space complexity**.

### Two-Pass Distributed Cache Correlation Pipeline
Evaluating word correlation $COR(A, B) = \frac{Freq(A, B)}{Freq(A) \times Freq(B)}$ across global corpora:
- **Pass 1:** Global word count persisted to intermediate HDFS storage.
- **Pass 2:** Reducer overrides `setup(Context context)` to preload intermediate frequency files directly into an in-memory lookup table via the Hadoop `FileSystem` API (Distributed Cache pattern).

```bash
# Build the MapReduce jar
cd mapreduce-patterns
mvn clean package

# Run Bigram Frequency Pairs
hadoop jar target/mapreduce-patterns-1.0.0.jar io.github.frieddeli.mapreduce.BigramFrequencyPairs \
  -input data/1400-8.txt -output output/pairs -numReducers 4
```

---

## 2. Spark RDD Optimization & Telemetry (`spark-telemetry/`)

### `groupByKey()` vs. `reduceByKey()` Shuffle Mechanics

```python
# ANTI-PATTERN: groupByKey()
words.map(lambda w: (w, 1)).groupByKey().mapValues(sum)

# OPTIMAL: reduceByKey()
words.map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)
```

- **`groupByKey()` Anti-Pattern:** Serializes every un-aggregated key-value tuple across executor partitions over the network. Causes excessive shuffle file generation, high garbage collection pauses, and driver/executor out-of-memory errors on high-cardinality keys.
- **`reduceByKey()` Combiner Optimization:** Performs map-side combiners within each executor partition prior to shuffle serialization, drastically slashing network transit and memory spill overhead.

### NASA Web Server Log Telemetry Pipeline
Processes unstructured HTTP access logs in Apache Common Log Format:
- Regex-based tokenization and structured tuple extraction across 3,597 unique client hosts.
- Strategic RDD caching (`access_logs.cache()`) across iterative analytical passes to prevent redundant disk re-reads under lazy evaluation.
- Diagnostic analysis identifying top failing endpoints and temporal 404 error spikes.

```bash
cd spark-telemetry
pip install -r requirements.txt
python word_count.py
python log_analysis.py
```

---

## 3. Cloud Hardware & Virtualization Benchmarks (`cloud-benchmarks/`)

Key empirical findings from benchmarking AWS EC2 compute, memory, and network subsystems:

1. **SMT Structural Contention:** On compute-bound prime calculations, two independent physical cores (`t2.medium`, Broadwell E5-2686 v4) achieved **$1.77\times$ scaling**, while two hyperthreads sharing a single core (`c5d.large`, Skylake 8124M) achieved only **$1.56\times$ scaling** due to execution unit hazards.
2. **Memory Bus Saturation (9× Disparity):** `c5d.large` achieved **$7,578.52\text{ MiB/s}$** memory throughput versus $855.68\text{ MiB/s}$ on `t2.medium`. Skylake provides **6 memory channels of DDR4-2666 ($127.9\text{ GB/s}$ theoretical peak)** versus 4 channels of DDR3-1600 ($51.2\text{ GB/s}$ peak).
3. **Network Proximity vs. WAN Transit:** Same-type instances within an AWS VPC achieved **$0.200\text{ ms}$ RTT**, while cross-continental transit between `us-east-1` (Virginia) and `us-west-2` (Oregon) suffered an **$89\%$ throughput collapse ($4.97\text{ Gbps} \rightarrow 529\text{ Mbps}$)** and a **$400\times$ latency penalty ($54.8\text{ ms}$)**.

Full data tables and microarchitecture analysis available in [`cloud-benchmarks/README.md`](cloud-benchmarks/README.md).

---

## License
MIT License. Copyright (c) 2026 Ray Shao.
