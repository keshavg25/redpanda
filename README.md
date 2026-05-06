# DS614 Big Data Engineering Project

## Redpanda Log-Structured Storage: Code-Level Analysis & Experimental Study

> ⚠️ **IMPORTANT:** Please switch to the `bigdata-experiments` branch to view all project work.

---

## Executive Summary

This project analyzes **Redpanda**, a Kafka-compatible distributed streaming system, by directly examining its source code and experimentally modifying its internal behavior.

Unlike documentation-based studies, this project **connects code → architecture → behavior** by tracing execution paths, identifying design decisions, and validating them through six controlled experiments.

The core mandatory experiment modifies Redpanda's log segment size from **128MB to 1MB**, forcing the system outside its optimal design to reveal performance trade-offs. Additional experiments cover throughput scaling, partition skew, log flush behavior, disk storage inspection, and message replay.

---

## What Problem Does Redpanda Solve?

Redpanda is designed for:

- High-throughput data ingestion
- Low-latency streaming
- Efficient disk usage

It replaces traditional Kafka architecture by eliminating JVM overhead and using a **native C++ log-structured storage engine**. It also removes the ZooKeeper dependency, simplifying deployment while improving performance.

---

## Core Concepts

### Event Streaming

Event streaming means continuously producing and consuming data in real time. Examples: user login events, payment transactions, IoT sensor readings, website clickstreams.

### Append-Only Log

Kafka and Redpanda use an append-only architecture where messages are never modified — only appended. This enables replay capability, reliability, fault tolerance, and auditability.

### Topic, Partition, and Offset

| Concept | Description |
|---------|-------------|
| **Topic** | A logical stream of messages (e.g., `payment-events`) |
| **Partition** | A topic is split into partitions for parallel processing; ordering guaranteed within a partition |
| **Offset** | Each message gets a unique sequence number; consumers track offsets to resume after failures |

---

## Execution Path (Code-Level)

A message follows this path through Redpanda's source:

| Step | File | Role |
|------|------|------|
| 1. Request Handling | `src/v/kafka/server/handlers/produce.cc` | Handles Kafka ProduceRequest |
| 2. Replication | `src/v/raft/consensus.cc` | Raft consensus for majority agreement |
| 3. Storage Engine | `src/v/storage/segment_appender.cc` | Appends data to log segments |
| 4. Disk Write | — | Uses **O_DIRECT** to bypass OS cache |

---

## Key Design Decisions

### 1. Log-Structured Storage
Sequential append-only writes optimized for streaming workloads.
**Trade-off:** Small segments lead to fragmentation and metadata overhead.

### 2. Direct I/O (O_DIRECT)
Avoids double buffering for predictable latency.
**Trade-off:** Less OS-level caching.

### 3. Thread-per-Core Architecture
Each CPU core runs independently with no shared state.
**Trade-off:** Sensitive to partition skew and uneven load.

---

## Concept Mapping

| Concept | Implementation |
|---------|----------------|
| Storage | Log-structured commit log |
| Execution | Event-driven pipeline |
| Streaming | Kafka-compatible ingestion |
| Partitioning | Key-based partitioning |
| Replication | Raft consensus |

---

## Experiments

---

### Experiment 1 — Segment Size Modification *(Mandatory)*

**Change:** Default segment size `128MB → 1MB`

**Method:** Modified Redpanda's internal configuration to reduce the log segment size threshold.

**Observation:**
- Large number of small `.log` files created per partition
- Increased metadata operations and file system pressure

**Insight:** Segment size is a critical tuning parameter. Smaller segments cause file explosion and metadata overhead, directly demonstrating the cost of breaking Redpanda's default design assumptions.

---

### Experiment 2 — Throughput vs. Load

**Method:** Scaled message volume from `1K → 100K` messages.

**Observation:**
- CPU utilization increased
- Disk I/O rates rose proportionally
- End-to-end latency increased at high load

**Insight:** Redpanda performance scales with system resources. The bottleneck shifts from network to disk I/O at high message volumes.

---

### Experiment 3 — Partition Skew

**Method:** All messages sent using the same key, forcing them to a single partition.

**Observation:**
- One partition became overloaded
- CPU imbalance across cores
- Other partitions remained idle

**Insight:** Poor key distribution eliminates parallelism. Balanced key distribution is essential for utilizing Redpanda's thread-per-core architecture effectively.

---

### Experiment 4 — Log Flush and Segment Persistence

**Objective:** Observe how Redpanda persists messages into disk log segments and performs flush operations.

**Theory:** Messages are first written to memory buffers for speed. Since memory is volatile, Redpanda periodically flushes buffers to persistent storage using write-ahead logging.

**Commands Used:**

```bash
# Create topic
bin/kafka-topics.sh --create \
  --topic test-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

# Produce 5000 messages
yes "test message" | head -n 5000 | kafkacat -b localhost:9092 -t test-topic

# Generate continuous load (10 rounds × 2000 messages)
for i in {1..10}; do
    yes "test message" | head -n 2000 | kafkacat -b localhost:9092 -t test-topic
    sleep 1
done
```

**Screenshot:**

![Experiment 4 - Log Flush](Screenshot%202026-05-06%20040746.png)

**Observations:**

The Redpanda logs showed `🔥 EXPERIMENT 4: FLUSH HAPPENED 🔥` at regular intervals, along with:
- New segment creation (`storage - segment.cc:851`)
- Snapshot creation (`cluster - controller_stm.cc:140`)
- Old segment removal (`disk_log_impl.cc:3281`)

**Offset types observed in logs:**

| Offset Type | Purpose |
|-------------|---------|
| `base_offset` | First offset in a segment |
| `committed_offset` | Messages safely committed to disk |
| `stable_offset` | Durable, readable state |

**Key Learning:** Redpanda uses write-ahead logging with periodic segment rollovers. Flushing ensures durability — messages survive crashes. The cycle of create → flush → remove segments is fundamental to the append-only storage design.

---

### Experiment 5 — Partition Storage and Internal Log Structure

**Objective:** Inspect how Redpanda stores partition data physically on disk.

**Theory:** Kafka-style topics are stored as directories on disk. Each partition is a separate append-only log with its own index structure, enabling horizontal scaling and parallel consumption.

**Commands Used:**

```bash
# Create topic
bin/kafka-topics.sh --create \
  --topic test-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

# List topics
bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Produce large volume
yes "test message" | head -n 100000 | kafkacat -b localhost:9092 -t test-topic

# Inspect storage directories
ls /tmp/redpanda-data/kafka/test-topic
ls -lh /tmp/redpanda-data/kafka/test-topic/0_7
```

**Screenshot:**

![Experiment 5 - Partition Storage](Screenshot%202026-05-06%20043649.png)

**Observed files:**

```
total 2.2M
-rw-r--r-- 1 gangw gangw  248 May  6 04:24  0-1-v1.base_index
-rw-r--r-- 1 gangw gangw 1.2M May  6 04:24  0-1-v1.log
-rw-r--r-- 1 gangw gangw 1.1M May  6 04:24  59268-1-v1.log
-rw-r--r-- 1 gangw gangw  136 May  6 04:24  stm_manager.snapshot
-rw-r--r-- 1 gangw gangw   53 May  6 04:24  tx.snapshot
```

**File roles:**

| File | Purpose |
|------|---------|
| `0-1-v1.log` | Actual message bytes, appended sequentially |
| `0-1-v1.base_index` | Maps offsets to physical byte positions for fast reads |
| `stm_manager.snapshot` | State machine recovery metadata |
| `tx.snapshot` | Transaction state for exactly-once semantics |

**Key Learning:** Topics are physically stored as directories; partitions are independent storage units. Index files prevent full scans during reads. Without balanced partitioning, one directory grows disproportionately, echoing the skew experiment above.

---

### Experiment 6 — Message Replay Using Offsets

**Objective:** Demonstrate Redpanda's replay capability using consumer offsets.

**Theory:** Unlike traditional systems that overwrite state, Kafka-style platforms preserve the full event history. Consumers maintain independent read positions (offsets) and can re-read any historical message.

**Commands Used:**

```bash
# Create replay topic
bin/kafka-topics.sh --create \
  --topic replay-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

# Produce three messages
echo "Message 1" | kafkacat -b localhost:9092 -t replay-topic
echo "Message 2" | kafkacat -b localhost:9092 -t replay-topic
echo "Message 3" | kafkacat -b localhost:9092 -t replay-topic

# Consume from the beginning (replay)
kafkacat -b localhost:9092 -t replay-topic -C -o beginning -e
```

**Screenshot:**

![Experiment 6 - Message Replay](Screenshot%202026-05-06%20045634.png)

**Observations:**

Both a Kafka CLI consumer and a Redpanda-native consumer replayed the messages in order:

```
Message 1
Message 2
Message 3
% Reached end of topic replay-topic [0] at offset 3: exiting
```

Multiple independent consumers replayed the same data without interfering with each other.

**Key Learning:** Replayability is a first-class feature. Offset-based consumption enables recovery after failures, state reconstruction, reprocessing with updated business logic, and auditability — all without any data loss.

---

## Failure Analysis

| Failure Case | Root Cause | Observed Effect |
|---|---|---|
| High Data Volume | CPU + I/O saturation | Increased latency, backpressure |
| Partition Skew | All messages on one key | Uneven load, idle cores |
| Small Segment Size | 128MB → 1MB config change | File explosion, metadata overhead |

---

## Output Evidence

Data stored in:

```bash
/tmp/redpanda-data/kafka/test-topic/
```

Observed partition directories, multiple segment files, index files, and snapshot metadata confirming the append-only log design in practice.

---

## Tools Used

| Tool | Purpose |
|------|---------|
| Redpanda | Kafka-compatible streaming platform (modified source) |
| Kafka CLI | Topic creation and management |
| kafkacat | Producing and consuming messages |
| Linux Commands | Inspecting storage files on disk |

---

## Key Insights

- Redpanda's performance is **design-driven, not accidental** — every tuning knob maps to a specific architectural trade-off
- Segment size is a **critical parameter** — breaking the 128MB default reveals cascading file system overhead
- Balanced partitioning is **essential** — key distribution directly determines whether parallelism is utilized
- Flush behavior and segment lifecycle are **observable** — the logs expose exactly when and why persistence occurs
- Offset-based consumption makes **replayability trivial** — any consumer can re-read any point in history independently

---

## Conclusion

Redpanda achieves high performance through sequential disk writes, controlled batching, direct I/O, and efficient partitioning. These six experiments validated the architectural design by breaking its assumptions and observing the predicted degradations:

- Shrinking segment size caused file explosion (Experiment 1)
- Skewed keys eliminated parallelism (Experiment 3)
- Flush logging confirmed write-ahead durability (Experiment 4)
- Physical file inspection confirmed the append-only log (Experiment 5)
- Offset replay confirmed independent consumer positioning (Experiment 6)

---

## Repository

[https://github.com/keshavg25/redpanda](https://github.com/keshavg25/redpanda)

---

## References

- Apache Kafka Documentation
- Redpanda Documentation
- *Fundamentals of Data Engineering* — Reis & Housley
- *Kafka: The Definitive Guide* — Shapira et al.
