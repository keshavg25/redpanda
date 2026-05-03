# Experiment 3: Partition Skew (Hot Key Problem)

##  Objective

To analyze how uneven key distribution (hot key) affects load balancing and resource utilization in Redpanda.

---

#Setup

* System: WSL2 (Ubuntu 22.04)
* Broker: Redpanda running locally
* Port: 9092
* Tool used: kafkacat
* Topic: test-topic

---

##  Methodology

### Step 1: Send messages using a fixed key (hot key)

```bash
yes "hotkey:test" | head -n 50000 | kafkacat -b localhost:9092 -t test-topic -K:
```

* All messages are sent with the same key
* Kafka partitioner maps same key → same partition

---

## Observations

* Most data is written to a single partition
* Uneven distribution of messages
* CPU usage is concentrated on one core
* Other cores remain underutilized

---

##  Analysis

* Kafka/Redpanda uses key-based partitioning
* Same key always goes to the same partition
* This creates a **hot partition**
* Leads to:

  * Load imbalance
  * Reduced parallelism
  * Performance bottleneck

---

##  Conclusion

Improper key distribution results in partition skew, where one partition becomes overloaded.
This reduces system efficiency and highlights the importance of proper key design in distributed streaming systems.

---
