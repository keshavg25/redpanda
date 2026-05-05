# Experiment 6: Consumer Offset and Message Replay

---

##  Objective

To analyze how Redpanda consumers read messages using offsets and how data can be replayed.

---

##  Concept

* Each message is stored with an offset
* Consumers read messages sequentially
* Resetting offset allows replay of data

---

##  Steps Performed

1. Started Redpanda server

2. Created topic:

   ```bash
   bin/kafka-topics.sh --create --topic replay-topic --bootstrap-server localhost:9092
   ```

3. Produced messages:

   ```bash
   echo "Message 1" | kafkacat ...
   echo "Message 2" | kafkacat ...
   echo "Message 3" | kafkacat ...
   ```

4. Consumed messages from beginning:

   ```bash
   kafkacat -b localhost:9092 -t replay-topic -C -o beginning -e
   ```

5. Repeated consumption:

   ```bash
   kafkacat -b localhost:9092 -t replay-topic -C -o beginning -e
   ```

---

##  Observation

Messages were read multiple times:

```text
Message 1
Message 2
Message 3
```

---

##  Analysis

* Logs are immutable
* Data is not deleted after consumption
* Offset controls what is read

---

##  Key Insight

> Redpanda allows replay of data using offsets, making it suitable for streaming and event-driven systems.

---

##  Conclusion

This experiment demonstrates how consumers interact with log storage and how message replay works using offsets.

---
