# Experiment 5: Log Segmentation in Redpanda

---

##  Objective

To understand how Redpanda manages log storage by splitting data into multiple segment files when the log size increases.

---

##  Concept

Redpanda stores messages in **log segments**.

* Each topic partition is stored as a sequence of log files.
* When a segment reaches a certain size limit, a **new segment file is created**.
* This helps in:

  * Efficient disk usage
  * Faster recovery
  * Better log management

---

## Configuration Change

The log segment size was reduced to force frequent segment creation.

File modified:

`conf/redpanda.yaml`

```yaml
redpanda:
  log_segment_size: 1048576   # 1 MB
```

---

##  Steps Performed

1. Cleaned existing data:

   ```bash
   rm -rf /tmp/redpanda-data
   mkdir -p /tmp/redpanda-data
   ```

2. Started Redpanda with modified configuration.

3. Created topic:

   ```bash
   bin/kafka-topics.sh --create \
   --topic test-topic \
   --bootstrap-server localhost:9092 \
   --partitions 1 \
   --replication-factor 1
   ```

4. Produced large amount of data:

   ```bash
   yes "test message" | head -n 100000 | kafkacat -b localhost:9092 -t test-topic
   ```

5. Checked log directory:

   ```bash
   ls -lh /tmp/redpanda-data/kafka/test-topic/<partition_folder>
   ```

---

##  Observation

Multiple log segment files were created:

```text
0-1-v1.log
1-2-v1.log
2-3-v1.log
```

Each file represents a segment of the log.

---

##  Analysis

* Smaller segment size → more frequent file creation
* Large data input → multiple segments generated
* Redpanda automatically manages segmentation

---

##  Key Insight

> Log segmentation allows Redpanda to efficiently manage large streams of data by splitting logs into smaller, manageable chunks.

---

##  Conclusion

This experiment demonstrates how Redpanda handles log storage internally using segmentation. By reducing the segment size, we were able to clearly observe the creation of multiple log files, validating the concept of log segmentation.

---
