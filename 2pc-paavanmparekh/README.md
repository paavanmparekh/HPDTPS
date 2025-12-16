
# High Performance Distributed Transaction Processing System

## 1. Introduction

This project implements a distributed transaction system using a Two-Phase Commit (2PC) protocol. The system is designed for high performance and fault tolerance, featuring configurable clusters and benchmarking capabilities to evaluate performance under different workloads. The project demonstrates a practical application of distributed consensus algorithms in ensuring data consistency across multiple nodes.

## 2. Project Structure

```
├───start_nodes.go
├───Client\
│   ├───benchmark-case1.json
│   ├───benchmark-case2.json
│   ├───benchmark-case3.json
│   ├───benchmark.go
│   └───main.go
├───Configurations\
│   ├───cluster_config.json
│   └───configurations.go
├───Input\
│   ├───CSE535-F25-Project-3-Testcases_flat.csv
│   └───CSE535-F25-Project-3-Testcases.csv
└───Node\
    ├───node.go
    ├───Commitment\
    │   └───commitment.go
    ├───Consensus\
    │   └───consensus.go
    └───logger\
        └───logger.go
```

- **`start_nodes.go`**: Main application to start the server nodes.
- **`Client/`**: Contains the client-side application that interacts with the 2PC system.
  - **`main.go`**: The main entry point for the client application.
  - **`benchmark.go`**: Contains logic for performance benchmarking.
- **`Node/`**: Contains the server-side application that participates in the 2PC protocol.
  - **`node.go`**: The main entry point for the node application.
  - **`Consensus/`**: Holds the core logic for the 2PC consensus algorithm.
  - **`Commitment/`**: Manages the commitment phase of the protocol.
  - **`logger/`**: Provides logging functionality.
- **`Configurations/`**: Manages cluster and system configurations.
  - **`cluster_config.json`**: Defines the nodes in the cluster.
- **`Input/`**: Contains test case data.

## 3. How to Setup and Run

### Prerequisites

- Go (latest version recommended)

### Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd 2pc-paavanmparekh
    ```

2.  **Build the applications:**

    Build the Node:
    ```bash
    go build -o Node/Node.exe Node/node.go
    ```

    Build the Client:
    ```bash
    go build -o Client/Client.exe Client/main.go
    ```

### Running the System

1.  **Start the Nodes:**
    Open a terminal and run the `start_nodes.go` script to launch the cluster nodes as defined in `Configurations/cluster_config.json`.
    ```bash
    go run start_nodes.go
    ```

2.  **Run the Client:**
    Open another terminal to run the client application. The client will send transaction requests to the nodes.
    ```bash
    ./Client/Client.exe
    ```

## 4. Bonuses Implemented

### High-Performance System with Benchmarking

The system is architected for high throughput and low latency. A dedicated benchmarking module (`Client/benchmark.go`) is included to measure the performance of the 2PC implementation. It supports different test cases (`benchmark-case1.json`, etc.) to simulate various workloads and system configurations, allowing for detailed performance analysis.

### Configurable Clusters

The cluster topology is not hardcoded. It can be easily defined and modified through the `Configurations/cluster_config.json` file. This allows for flexible testing of the 2PC protocol with different numbers of nodes and network configurations, showcasing the system's scalability and adaptability.

## 5. Results
Benchmarking Results: 
| Benchmark Case | Total Transactions | Throughput (txn/s) | Avg Latency (ms) | Wall-Clock Duration |
| -------------- | ------------------ | ------------------ | ---------------- | ------------------- |
| Case 1         | 200                | **3302.30**        | 30.52            | 60.56 ms            |
| Case 2         | 2000               | **999.84**         | 83.62            | 2.00 s              |
| Case 3         | 30000              | **1342.23**        | 168.95           | 22.35 s             |

## 6. Acknowledgement

The development of this project was assisted by AI. AI was used in implimenting skeleton code for the protocols implimented, resolving errors, debugging code. Some of the feaures like resharding mechanism, benchmarking and optimizations to improve the overall performance of the system are assisted by AI.
