# Distributed Systems

This repository contains the implementations for the **Distributed Systems course project** at **UIUC**. The project consists of multiple **MPs**, each focusing on different aspects of distributed systems, including membership protocols, consistency, failure detection, and distributed processing frameworks.

## Project Overview

The project is divided into **four** main parts:

1. **MP1 - Distributed Grep Service**
2. **MP2 - SWIM Membership Protocol**
3. **MP3 - Distributed File System with Consistency Guarantees**
4. **MP4 - Distributed Stream Processing (RainStorm vs Spark)**

Each MP builds upon the previous one, demonstrating core distributed system concepts.

---

## MP1: Distributed Grep Service

**Objective**: Implement a **client-server grep service** where a client sends a `grep` command to multiple servers, which execute the search on local log files and return the results.

### Features:
- **Client-server model**: Client sends grep queries to multiple servers over a network.
- **Parallel execution**: Each server processes the grep command independently.
- **Fault tolerance**: If a server crashes, the system continues operation silently.

### How to Run:
```sh
python3 server.py &  # Start the server on multiple machines
python3 client.py <grep command>  # Send grep command to servers
```

---

## MP2: SWIM Membership Protocol

**Objective**: Implement a **failure detector and membership protocol** using **SWIM (Scalable Weakly-consistent Infection-style Membership)** with **time-bound completeness**.

### Features:
- **Failure detection** using SWIM with broadcasting.
- **Suspicion mechanism** to reduce false positives.
- **UDP-based communication** for efficient message exchange.

### How to Run:
```sh
python3 start_server.py --node-id 01 --is-introducer
python3 start_server.py --node-id XX  # Start additional nodes
```

---

## MP3: Distributed File System with Consistency Guarantees

**Objective**: Implement a **simple distributed file system** with **replication, consistency, and re-replication** mechanisms.

### Features:
- **3-replica model** to handle up to **2 simultaneous failures**.
- **Read/Write consistency**: Uses `ALL` for writes and `ONE` for reads.
- **Merge mechanism**: Ensures replicas have consistent data.
- **Re-replication**: When a node fails, remaining nodes redistribute files.

### How to Run:
```sh
python3 dfs_server.py  # Start distributed file system
python3 dfs_client.py put <file>  # Upload a file
python3 dfs_client.py get <file>  # Retrieve a file
```

---

## MP4: Distributed Stream Processing - RainStorm vs Spark

**Objective**: Implement **RainStorm**, a distributed stream processing framework, and compare it with Apache Spark.

### Features:
- **Leader-based task scheduling**: Dynamically assigns tasks to workers.
- **Exactly-once processing**: Avoids duplicate records using unique IDs.
- **Failure recovery**: Restores lost state from logs stored in HDFS.
- **Performance comparison**: Benchmarks RainStorm against Spark.

### How to Run:
```sh
python3 rainstorm.py --dataset <input_file>
```

---

## Performance Analysis

### Key Findings:
- **MP2 (SWIM)**: Suspicion mechanism reduces false positives but increases message overhead.
- **MP3 (DFS)**: Read/write consistency is ensured with minimal overhead, but large file re-replication introduces delays.
- **MP4 (RainStorm)**: Spark generally outperforms RainStorm, but custom optimizations could improve performance.

---

## Repository Structure

```
/src
  ├── mp1/   # Distributed Grep
  ├── mp2/   # SWIM Membership Protocol
  ├── mp3/   # Distributed File System
  ├── mp4/   # RainStorm (Distributed Stream Processing)
```

## Authors
- **Wenjie Sun**
- **Hung-Jui Chen**

---

## Dependencies
- Python 3
- UDP/TCP networking (for MP1 & MP2)
- HDFS (for MP3 & MP4)
- Apache Spark (for comparison in MP4)

---

## References
- SWIM: Scalable Weakly-consistent Infection-style Membership
- Apache Spark: Distributed Data Processing


