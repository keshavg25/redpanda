# Execution Path, Design Decisions & Concept Mapping

## 1️⃣ Execution Path (Write Path)

Producer sends message → Redpanda processes request → data stored on disk.

### Step-by-step flow:

1. **Kafka Request Handling**

   * File: `src/v/kafka/server/handlers/produce.cc`
   * Handles incoming ProduceRequest from client

2. **Raft Consensus Layer**

   * File: `src/v/raft/consensus.cc`
   * Function: AppendEntries
   * Ensures replication across nodes

3. **Storage Layer**

   * File: `src/v/storage/segment_appender.cc`
   * Appends data to log segment files

4. **Disk I/O**

   * Uses direct I/O (O_DIRECT)
   * Bypasses OS page cache

---

## 2️⃣ Design Decisions

### 🔹 1. Log-Structured Storage

* Data written sequentially in segments
* Improves write throughput

**Tradeoff:**

* Fragmentation when segments are small

---

### 🔹 2. Direct I/O (O_DIRECT)

* Avoids double buffering in OS cache

**Tradeoff:**

* Requires careful memory management

---

### 🔹 3. Thread-per-Core Model

* Each CPU core runs independently

**Tradeoff:**

* Load imbalance under skew

---

## 3️⃣ Concept Mapping

| Concept      | Implementation            |
| ------------ | ------------------------- |
| Storage      | Log-structured commit log |
| Execution    | Event-driven processing   |
| Streaming    | Kafka-based ingestion     |
| Partitioning | Key-based partition model |
| Replication  | Raft consensus            |

---

## 4️⃣ Failure Analysis

### 🔸 Case 1: High Data Volume

* CPU and disk usage increase
* Throughput slows down

### 🔸 Case 2: Partition Skew

* One partition overloaded
* Uneven CPU utilization

### 🔸 Case 3: Small Segment Size

* Too many files created
* Increased metadata overhead

---
