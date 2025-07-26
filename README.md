# 6.5840 Lab 1: MapReduce (MIT Distributed Systems)

This is my implementation of Lab 1 for MIT's [6.5840 Distributed Systems](https://pdos.csail.mit.edu/6.5840/) course. The lab implements a simplified version of the MapReduce framework in Go, including a fault-tolerant coordinator and parallel workers using RPC.

## 📚 Overview

This project builds a basic MapReduce system in Go with the following features:

- A **coordinator** that assigns and manages Map and Reduce tasks.
- Multiple **workers** that request and execute tasks in parallel.
- **Fault-tolerance** via task re-assignment on timeout.
- A **plugin mechanism** for defining custom `Map` and `Reduce` logic.

## 🏗️ Architecture

- The **Coordinator**:
  - Launches with a list of input files.
  - Assigns Map tasks to workers.
  - Waits for all Map tasks to complete.
  - Then assigns Reduce tasks.
  - Exits when all tasks are done.

- The **Worker**:
  - Continuously requests tasks via RPC.
  - Executes the user-defined `Map` or `Reduce` function from a shared object file.
  - Writes intermediate or final output files.
  - Reports completion status back to the coordinator.

## 🚀 How to Use

### 🧱 Requirements

- Go (version 1.18+)
- A Unix-like system (Linux, macOS, or WSL)

---

### 🧩 Step 1: Write Your Map and Reduce Functions

Create a Go file that defines two functions with the following exact signatures:

```go
package main

import "6.5840/mr" // or adjust import path based on your structure

func Map(filename string, contents string) []mr.KeyValue {
    // Your custom logic
}

func Reduce(key string, values []string) string {
    // Your custom logic
}
