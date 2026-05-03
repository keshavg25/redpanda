# Experiment 2: Throughput vs Load Analysis

##  Objective

To analyze how Redpanda performs under different data loads and observe the impact on system resources.

---

##  Setup

* System: WSL2 (Ubuntu 22.04)
* Broker: Redpanda running locally
* Port: 9092
* Tool used: kafkacat

---

##  Methodology

Three levels of load were tested:

### 🔹 Low Load

```bash
yes "msg" | head -n 1000 | kafkacat -b localhost:9092 -t test-topic
```

### 🔹 Medium Load

```bash
yes "msg" | head -n 10000 | kafkacat -b localhost:9092 -t test-topic
```

### 🔹 High Load

```bash
yes "msg" | head -n 100000 | kafkacat -b localhost:9092 -t test-topic
```

---

##  Observations

| Load Level | Messages | CPU Usage | Disk Activity | Time Taken |
| ---------- | -------- | --------- | ------------- | ---------- |
| Low        | 1,000    | Low       | Low           | Fast       |
| Medium     | 10,000   | Moderate  | Moderate      | Moderate   |
| High       | 100,000  | High      | High          | Slow       |

* CPU usage increased with higher load
* Disk writes increased significantly
* Processing time increased with message volume

---

##  Analysis

* Redpanda processes data efficiently at low load
* As load increases, resource utilization increases
* High throughput leads to increased disk I/O and CPU usage

---

##  Conclusion

System performance is directly affected by load.
Higher message volume increases CPU usage, disk activity, and processing time, demonstrating the importance of resource management in streaming systems.

---
