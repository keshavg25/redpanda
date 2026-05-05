# Experiment 4: Visualizing Log Flush Operations

## Objective

To observe when data is flushed to disk by modifying the source code.

---

## Modification

Added log in:
`src/v/storage/segment_appender.cc`

```cpp
std::cout << "FLUSH HAPPENED" << std::endl;
```

---

## Observation

During message production:

* Log messages appeared in terminal
* Each message corresponds to a flush operation

---

## Insight

* Flush does not happen per message
* It is controlled internally for efficiency

---

## Conclusion

This experiment successfully visualized the internal flush mechanism of Redpanda's log system.
