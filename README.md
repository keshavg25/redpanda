# 🦆 DS614 Big Data Engineering: Redpanda Log-Structured Storage Analysis

**Team:** THE DATA DUO
**Members:** KESHAV GANGWANI , ANUSHA SUNGH
**System Analyzed:** Redpanda — Kafka-compatible streaming platform
**Repository:** https://github.com/keshavg25/redpanda
**Base Source:** https://github.com/redpanda-data/redpanda

---

#  Executive Summary

Modern streaming systems rely on log-structured storage for high-throughput data ingestion. Redpanda implements this using segmented append-only logs, optimized for sequential disk access and low-latency processing.

This project goes beyond black-box usage by **modifying Redpanda’s source code**, rebuilding the system, and empirically analyzing how internal design decisions affect storage behavior, performance, and load distribution.

The core experiment involves reducing log segment size from **128MB to 1MB**, forcing the system into a non-optimal configuration to observe trade-offs in fragmentation, metadata overhead, and disk I/O patterns.

> If you cannot connect behavior to source-level design, you have not understood the system.

---

#  System Overview

Redpanda is a **Kafka-compatible distributed streaming engine** designed for high throughput without JVM overhead.

### Key Characteristics:

* Log-structured storage model
* Partitioned data streams
* Append-only segment files
* Kafka API compatibility

---

#  Environment Setup

| Component      | Details             |
| -------------- | ------------------- |
| OS             | WSL2 (Ubuntu 22.04) |
| Build System   | Bazel               |
| Language       | C++                 |
| Messaging Tool | kafkacat            |
| Broker Port    | 9092                |

---

#  Experiments

---

##  Experiment 1 — Log Segment Size Modification (Core Experiment)

### Objective

To analyze how segment size impacts storage behavior and disk efficiency.

### Method

* Modified Redpanda source code
* Changed segment size from:

  ```text
  128MB → 1MB
  ```
* Rebuilt system using Bazel
* Produced 50,000 messages

### Observation

* Large number of small `.log` files created
* Increased file system operations
* Higher metadata overhead

### Analysis

Segment size directly affects:

* Disk fragmentation
* Write amplification
* File system pressure

### Conclusion

Reducing segment size degrades storage efficiency, demonstrating the importance of batching in log-structured systems.

---

##  Experiment 2 — Throughput vs Load

### Objective

To evaluate system performance under increasing data volume.

### Method

* Generated workloads of:

  * 1,000 messages
  * 10,000 messages
  * 100,000 messages

### Observations

* CPU usage increased with load
* Disk I/O increased significantly
* Processing time scaled with message volume

### Analysis

Throughput is constrained by:

* CPU availability
* Disk bandwidth
* Message batching efficiency

### Conclusion

System performance scales with load but introduces resource pressure at higher volumes.

---

##  Experiment 3 — Partition Skew (Hot Key Problem)

### Objective

To analyze uneven load distribution across partitions.

### Method

* Sent all messages using a fixed key

### Observations

* Data concentrated in a single partition
* Uneven CPU utilization
* Reduced parallelism

### Analysis

Kafka-style partitioning:

* Same key → same partition
* Causes hotspot formation

### Conclusion

Improper key distribution leads to performance bottlenecks and inefficient resource usage.

---

#  Storage Analysis

Data stored at:

```bash
/tmp/redpanda-data/kafka/test-topic/
```

### Observed Structure:

* Partition directories
* Multiple segment files
* Increased file count due to reduced segment size

---

#  Concept Mapping

| Concept                | Implementation in Redpanda       |
| ---------------------- | -------------------------------- |
| Log-Structured Storage | Append-only segment files        |
| Partitioning           | Kafka-compatible partition model |
| Throughput Scaling     | Sequential disk writes           |
| Load Balancing         | Key-based partition assignment   |

---

#  Key Findings

| Observation                           | Insight             |
| ------------------------------------- | ------------------- |
| Smaller segments → more files         | Increased overhead  |
| Higher load → higher CPU & disk usage | Resource dependency |
| Hot key → uneven partition usage      | Load imbalance      |

---

# Failure Analysis

### 1. Small Segment Size

* Excessive file creation
* Increased metadata operations
* Reduced disk efficiency

### 2. High Load

* CPU saturation
* Disk bottleneck

### 3. Partition Skew

* Underutilized resources
* Bottleneck on single partition

---

#  Conclusion

Redpanda’s performance is strongly influenced by its log-structured design.
The system is optimized for:

* Large segment sizes
* Sequential disk access
* Balanced partitioning

Breaking these assumptions exposes clear performance degradation, validating the architectural trade-offs behind Redpanda’s design.

---

#  References

* https://github.com/redpanda-data/redpanda
* https://docs.redpanda.com

---

