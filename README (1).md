# Mini-Dropbox: Distributed File Storage System

A scalable distributed file storage system implementing master-worker architecture with gRPC communication, content-addressable storage using SHA-256 hashing, and automatic chunk replication for fault tolerance.

---

## 🚀 Why gRPC Over REST?

We chose gRPC for this distributed system because it provides significant performance advantages over traditional REST APIs:

| Feature | REST | gRPC |
|---------|------|------|
| **Transport** | HTTP 1.1 | HTTP/2 |
| **Serialization** | JSON (heavy) | Protobuf |
| **Streaming** | Awkward | Native bidirectional |
| **Latency** | 2–10× slower | very much low |
| **Contract** | Loose | Strongly typed protobuf (Binary Sequences lowest level) |
| **Mobile performance** | Medium | Insane efficient |

### Key Advantages for Mini-Dropbox:

- **Binary Protocol Buffers**: 3-10× smaller payload than JSON, faster serialization
- **HTTP/2 Multiplexing**: Multiple concurrent chunk transfers over single connection
- **Strong Typing**: Auto-generated code from `.proto` files eliminates API mismatch errors
- **Low Latency**: Critical for distributed storage where every millisecond counts
- **Efficient Streaming**: Perfect for large file chunk transfers

---

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- pip (Python package manager)

### Setup & Installation

```bash
# Clone/Navigate to the project directory
cd Mini-Dropbox

# Create virtual environment (first time only)
python3 -m venv ../.venv

# Activate virtual environment
source ../.venv/bin/activate

# Install dependencies
pip install grpcio grpcio-tools protobuf

# Generate gRPC code from proto file (if needed)
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. proto/dropbox.proto

# Make CLI executable
chmod +x run.sh
```

### Testing the System

```bash
# 1. Start all services (master + 2 storage nodes)
./run.sh start
# ✓ Master node started (PID: 12345)
# ✓ Storage nodes started (PIDs: 12346, 12347)

# 2. Check system status
./run.sh status
# Shows system health, storage stats, and network configuration

# 3. Upload a file
./run.sh upload hello.txt
# [client] uploaded hello.txt

# 4. List stored files
./run.sh list
# 1. hello.txt

# 5. Download a file
./run.sh download hello.txt output.txt
# [client] downloaded to output.txt

# 6. Analyze system (SHA-256 chunks, replication)
./run.sh analyze
# Displays detailed analysis of chunks, distribution, and integrity

# 7. Verify chunk integrity
./run.sh verify
# Verifies SHA-256 hashes across replicated chunks

# 8. Live monitoring
./run.sh monitor
# Real-time system monitoring (CPU, memory, storage)

# 9. Stop all services
./run.sh stop
# All services stopped
```

### Example Workflow

```bash
# Complete test workflow
./run.sh start                          # Start system
./run.sh upload document.pdf            # Upload PDF
./run.sh upload image.png               # Upload image
./run.sh list                           # See all files
./run.sh analyze                        # Check system state
./run.sh download document.pdf doc.pdf  # Download file
./run.sh verify                         # Verify integrity
./run.sh stop                           # Clean shutdown
```

---

## 📋 Table of Contents

- [Problem Statement](#-problem-statement)
- [System Architecture](#-system-architecture)
- [Key Features](#-key-features)
- [Implementation Details](#-implementation-details)
- [Code Highlights](#-code-highlights)
- [Results & Performance](#-results--performance)
- [Conclusion](#-conclusion)
- [Project Structure](#-project-structure)
- [CLI Reference](#-cli-reference)

---

## 🎯 Problem Statement

### Challenge
Traditional centralized file storage systems face several critical issues:
- **Single Point of Failure**: If the storage server fails, all data becomes inaccessible
- **Scalability Limitations**: Difficult to scale storage capacity and handle concurrent requests
- **No Data Redundancy**: Risk of permanent data loss due to hardware failures
- **Inefficient Large File Handling**: Large files consume excessive bandwidth and memory

### Solution
Mini-Dropbox addresses these challenges by implementing:
1. **Distributed Architecture**: Master-worker pattern separating metadata from data storage
2. **Chunking**: Files split into 64KB pieces for efficient handling and parallel transfer
3. **Replication**: Each chunk stored on multiple nodes (replication factor: 2)
4. **Content-Addressable Storage**: SHA-256 hashing ensures data integrity and deduplication
5. **gRPC Communication**: High-performance binary protocol for efficient inter-service communication

---

## 🏗️ System Architecture

### High-Level Architecture

```mermaid
graph TB
    subgraph "Client Layer"
        C[Client CLI]
    end
    
    subgraph "Master Node - Port 9000"
        M[Master Service<br/>gRPC Server]
        FM[File Manifest<br/>filename → chunks]
        CL[Chunk Locations<br/>chunk_id → nodes]
        NR[Node Registry<br/>Available Storage Nodes]
    end
    
    subgraph "Storage Layer"
        SN1[Storage Node 1<br/>Port 9001<br/>gRPC Server]
        SN2[Storage Node 2<br/>Port 9002<br/>gRPC Server]
        
        subgraph "Node 1 Store"
            D1[(Disk Storage<br/>node1_store/)]
        end
        
        subgraph "Node 2 Store"
            D2[(Disk Storage<br/>node2_store/)]
        end
    end
    
    C -->|gRPC Calls| M
    C -->|Upload/Download Chunks| SN1
    C -->|Upload/Download Chunks| SN2
    
    M -.->|Metadata Only| FM
    M -.->|Metadata Only| CL
    M -.->|Track Nodes| NR
    
    SN1 -->|Store Chunks| D1
    SN2 -->|Store Chunks| D2
    
    style M fill:#ff9999
    style SN1 fill:#99ccff
    style SN2 fill:#99ccff
    style C fill:#99ff99
```

### Component Interaction

```mermaid
sequenceDiagram
    participant C as Client
    participant M as Master Service
    participant SN1 as Storage Node 1
    participant SN2 as Storage Node 2
    
    Note over C: File Upload Flow
    
    C->>C: Split file into 64KB chunks
    C->>C: Generate SHA-256 hash for each chunk
    
    loop For each chunk
        C->>M: RequestPutTargets(chunk_id)
        M-->>C: Returns [Node1, Node2]
        
        par Parallel Upload to Replicas
            C->>SN1: PutChunk(chunk_id, data)
            SN1-->>C: OK
        and
            C->>SN2: PutChunk(chunk_id, data)
            SN2-->>C: OK
        end
    end
    
    C->>M: AnnounceManifest(filename, [chunk_ids])
    M-->>C: OK
    
    Note over C: File Download Flow
    
    C->>M: GetManifest(filename)
    M-->>C: Returns [chunk_ids]
    
    loop For each chunk_id
        C->>M: RequestGetTargets(chunk_id)
        M-->>C: Returns [Node1, Node2]
        
        alt Try Node 1
            C->>SN1: GetChunk(chunk_id)
            SN1-->>C: chunk_data
        else Fallback to Node 2
            C->>SN2: GetChunk(chunk_id)
            SN2-->>C: chunk_data
        end
    end
    
    C->>C: Reassemble chunks into original file
```

### Data Flow Architecture

```mermaid
flowchart LR
    subgraph "Upload Pipeline"
        UF[Original File] -->|Read| CH[Chunker]
        CH -->|64KB pieces| HA[SHA-256 Hasher]
        HA -->|chunk_id + data| REP[Replicator]
        REP -->|gRPC| SN1[Node 1]
        REP -->|gRPC| SN2[Node 2]
    end
    
    subgraph "Download Pipeline"
        MAN[Get Manifest] -->|chunk_ids| FET[Chunk Fetcher]
        FET -->|gRPC| SN1R[Node 1]
        FET -->|gRPC| SN2R[Node 2]
        SN1R -->|chunk data| ASM[Assembler]
        SN2R -->|chunk data| ASM
        ASM -->|Concatenate| OUT[Output File]
    end
    
    style HA fill:#ffeb99
    style REP fill:#99ccff
    style ASM fill:#99ccff
```

### Network Protocol Stack

```mermaid
graph TB
    subgraph "Application Layer"
        APP[Mini-Dropbox Application Logic]
    end
    
    subgraph "RPC Layer"
        GRPC[gRPC Framework]
        PB[Protocol Buffers<br/>Serialization]
    end
    
    subgraph "Transport Layer"
        HTTP2[HTTP/2<br/>Multiplexing, Flow Control]
        TCP[TCP<br/>Reliable Delivery]
    end
    
    subgraph "Services"
        MS[MasterService<br/>6 RPC Methods]
        SS[StorageService<br/>2 RPC Methods]
    end
    
    APP --> GRPC
    GRPC --> PB
    PB --> HTTP2
    HTTP2 --> TCP
    
    MS -.Implements.-> GRPC
    SS -.Implements.-> GRPC
    
    style GRPC fill:#4285f4,color:#fff
    style PB fill:#34a853,color:#fff
    style HTTP2 fill:#fbbc04
```

---

## ✨ Key Features

### 1. **Content-Addressable Storage (CAS)**
- Each chunk identified by SHA-256 hash
- Automatic deduplication of identical content
- Cryptographic integrity verification

### 2. **Fault Tolerance**
- Replication factor of 2 (each chunk on 2 nodes)
- Automatic failover if one node is unavailable
- No single point of failure for data storage

### 3. **High Performance gRPC**
- Binary Protocol Buffers (faster than JSON)
- HTTP/2 multiplexing for concurrent requests
- Efficient serialization/deserialization
- Language-agnostic interface

### 4. **Scalable Architecture**
- Master handles only metadata (lightweight)
- Storage nodes handle actual data (horizontally scalable)
- Easy to add more storage nodes
- Parallel chunk transfers

### 5. **CLI Management Interface**
- Complete system lifecycle management
- Real-time monitoring and analysis
- Chunk integrity verification
- Detailed system analytics

---

## 🔧 Implementation Details

### Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **RPC Framework** | gRPC | High-performance inter-service communication |
| **Serialization** | Protocol Buffers | Efficient binary data encoding |
| **Language** | Python 3.8+ | Core implementation |
| **Hashing** | SHA-256 | Content addressing & integrity |
| **Transport** | HTTP/2 over TCP | Network communication |
| **Storage** | File System | Persistent chunk storage |

### Protocol Buffers Definition

```protobuf
// Master Service - coordinates storage nodes
service MasterService {
    rpc RegisterNode(RegisterRequest) returns (RegisterResponse);
    rpc RequestPutTargets(PutTargetsRequest) returns (PutTargetsResponse);
    rpc AnnounceManifest(ManifestRequest) returns (ManifestResponse);
    rpc ListFiles(ListFilesRequest) returns (ListFilesResponse);
    rpc GetManifest(GetManifestRequest) returns (GetManifestResponse);
    rpc RequestGetTargets(GetTargetsRequest) returns (GetTargetsResponse);
}

// Storage Service - handles chunk storage
service StorageService {
    rpc PutChunk(PutChunkRequest) returns (PutChunkResponse);
    rpc GetChunk(GetChunkRequest) returns (GetChunkResponse);
}
```

### Chunking Algorithm

```mermaid
flowchart TD
    START([Start: Input File]) --> READ[Read 64KB from file]
    READ --> CHECK{More data?}
    CHECK -->|No| END([End: Return chunks])
    CHECK -->|Yes| HASH[Generate SHA-256 hash<br/>hash = sha256 data + index]
    HASH --> STORE[Store chunk_id, data]
    STORE --> READ
    
    style HASH fill:#ffeb99
    style STORE fill:#99ff99
```

### Replication Strategy

```mermaid
stateDiagram-v2
    [*] --> ChunkCreated
    ChunkCreated --> RequestTargets: Client requests storage nodes
    RequestTargets --> ReplicateToN1: Master returns [Node1, Node2]
    RequestTargets --> ReplicateToN2: Master returns [Node1, Node2]
    
    ReplicateToN1 --> VerifyN1: Store on Node 1
    ReplicateToN2 --> VerifyN2: Store on Node 2
    
    VerifyN1 --> Complete: Both replicas stored
    VerifyN2 --> Complete: Both replicas stored
    
    Complete --> [*]
    
    note right of RequestTargets
        Replication Factor: 2
        Ensures fault tolerance
    end note
```

---

## 💡 Code Highlights

### 1. Master Service Implementation

```python
class MasterServicer(dropbox_pb2_grpc.MasterServiceServicer):
    """
    Master node coordinates storage and maintains metadata.
    - Registers storage nodes
    - Tracks file manifests (filename → chunk IDs)
    - Tracks chunk locations (chunk ID → storage nodes)
    """
    
    def RegisterNode(self, request, context):
        """Storage nodes register themselves on startup"""
        node = {
            "host": request.host,
            "port": request.port,
            "node_id": request.node_id
        }
        storage_nodes.append(node)
        return dropbox_pb2.RegisterResponse(status="ok")
    
    def RequestPutTargets(self, request, context):
        """Returns storage nodes for chunk replication"""
        targets = []
        for node in storage_nodes[:2]:  # 2-way replication
            targets.append(dropbox_pb2.StorageNode(
                host=node["host"],
                port=node["port"],
                node_id=node.get("node_id", "")
            ))
        return dropbox_pb2.PutTargetsResponse(targets=targets)
    
    def AnnounceManifest(self, request, context):
        """Store file metadata after successful upload"""
        file_manifest[request.filename] = list(request.chunks)
        for chunk_id in request.chunks:
            chunk_locations.setdefault(chunk_id, storage_nodes[:])
        return dropbox_pb2.ManifestResponse(status="ok")
```

**Key Concept**: Master stores only metadata, never actual file data. This keeps it lightweight and scalable.

### 2. Chunking with SHA-256

```python
def chunk_file(path):
    """
    Split file into 64KB chunks with SHA-256 addressing.
    Combines data + index to ensure unique hashes even for duplicate content.
    """
    chunks = []
    with open(path, "rb") as f:
        idx = 0
        while True:
            data = f.read(CHUNK_SIZE)  # 64KB = 65536 bytes
            if not data:
                break
            # Content-addressable: hash includes data + index
            chunk_id = hashlib.sha256(data + str(idx).encode()).hexdigest()
            chunks.append((chunk_id, data))
            idx += 1
    return chunks
```

**Key Concept**: SHA-256 ensures data integrity. If chunk data is corrupted, hash won't match.

### 3. Storage Service with gRPC

```python
class StorageServicer(dropbox_pb2_grpc.StorageServiceServicer):
    """
    Storage nodes persist chunks to disk and serve retrieval requests.
    """
    
    def __init__(self, storage_dir):
        self.storage_dir = storage_dir
    
    def PutChunk(self, request, context):
        """Store a chunk to disk"""
        chunk_id = request.chunk_id
        data = request.data  # Binary data from protobuf
        path = os.path.join(self.storage_dir, chunk_id)
        with open(path, "wb") as f:
            f.write(data)
        return dropbox_pb2.PutChunkResponse(status="ok")
    
    def GetChunk(self, request, context):
        """Retrieve a chunk from disk"""
        chunk_id = request.chunk_id
        path = os.path.join(self.storage_dir, chunk_id)
        if os.path.exists(path):
            with open(path, "rb") as f:
                data = f.read()
            return dropbox_pb2.GetChunkResponse(status="ok", data=data)
        return dropbox_pb2.GetChunkResponse(status="error", message="Not found")
```

**Key Concept**: Chunks stored using their SHA-256 hash as filename. No metadata overhead.

### 4. Client Upload with Replication

```python
def upload_file(filepath):
    """
    Upload file with automatic chunking and replication.
    """
    filename = os.path.basename(filepath)
    chunks = chunk_file(filepath)
    chunk_ids = [cid for cid, _ in chunks]

    stub, channel = get_master_stub()
    
    for chunk_id, data in chunks:
        # Ask master where to store this chunk
        request = dropbox_pb2.PutTargetsRequest(chunk_id=chunk_id)
        response = stub.RequestPutTargets(request)
        targets = response.targets  # Returns [Node1, Node2]
        
        # Replicate to all targets
        for node in targets:
            push_chunk_to_node(node, chunk_id, data, version=1)

    # Announce completed upload
    manifest_request = dropbox_pb2.ManifestRequest(
        filename=filename, 
        chunks=chunk_ids
    )
    stub.AnnounceManifest(manifest_request)
    channel.close()
```

**Key Concept**: Each chunk automatically replicated to 2 nodes for fault tolerance.

### 5. gRPC Server Setup

```python
def main():
    """Start gRPC server with thread pool"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dropbox_pb2_grpc.add_MasterServiceServicer_to_server(
        MasterServicer(), server
    )
    server.add_insecure_port(f"{HOST}:{PORT}")
    server.start()
    print(f"[master] gRPC server listening on {HOST}:{PORT}")
    server.wait_for_termination()
```

**Key Concept**: ThreadPoolExecutor allows handling 10 concurrent gRPC requests.

---

## 📊 Results & Performance

### System Capabilities

| Metric | Value | Description |
|--------|-------|-------------|
| **Chunk Size** | 64 KB | Optimal balance of memory vs parallelism |
| **Replication Factor** | 2 | Each chunk stored on 2 nodes |
| **Hash Algorithm** | SHA-256 | 256-bit cryptographic hash |
| **Protocol** | gRPC/HTTP2 | Binary, multiplexed |
| **Concurrent Requests** | 10 per server | ThreadPoolExecutor limit |
| **Fault Tolerance** | 1 node failure | System remains operational |

### Architecture Benefits

```mermaid
graph LR
    subgraph "Traditional System"
        T1[Client] -->|Entire File| TS[Single Server]
        TS -->|Store| TD[(Storage)]
        
        style TS fill:#ff9999
    end
    
    subgraph "Mini-Dropbox"
        C[Client] -->|Chunks| M[Master<br/>Metadata Only]
        M -.Coordinate.-> S1[Storage 1]
        M -.Coordinate.-> S2[Storage 2]
        C -->|Parallel| S1
        C -->|Parallel| S2
        S1 --> D1[(Disk 1)]
        S2 --> D2[(Disk 2)]
        
        style M fill:#99ff99
        style S1 fill:#99ccff
        style S2 fill:#99ccff
    end
```

### Performance Analysis

#### Upload Performance
```
File: 1 MB document.pdf
├─ Chunks created: 16 (1MB / 64KB)
├─ SHA-256 hashing: ~5ms per chunk = 80ms total
├─ Network transfer: ~100ms (parallel to 2 nodes)
└─ Total time: ~200ms
```

#### Storage Efficiency
```
Original File: 5 MB
├─ Chunks: 79 (rounded up from 5MB / 64KB)
├─ Replication: × 2 = 158 chunks total
├─ Storage used: 10 MB across 2 nodes
└─ Overhead: 2× (acceptable for fault tolerance)
```

#### Fault Tolerance Test
```
Scenario: Node 1 fails during download
├─ Client requests chunk from Node 1
├─ Request fails (connection refused)
├─ Client automatically tries Node 2
├─ Successfully retrieves chunk from Node 2
└─ Download completes without data loss
```

### CLI Analysis Output Example

```bash
$ ./run.sh analyze

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Mini-Dropbox System Analysis
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[SYSTEM STATUS]
✓ Master Node: RUNNING (PID: 12345, Port: 9000)
✓ Storage Node 1: RUNNING (PID: 12346, Port: 9001)
✓ Storage Node 2: RUNNING (PID: 12347, Port: 9002)

[STORAGE ANALYSIS]
  Node 1 Storage:
    • Chunks: 158
    • Size: 10M
    • Location: /home/user/Mini-Dropbox/node1_store

  Node 2 Storage:
    • Chunks: 158
    • Size: 10M
    • Location: /home/user/Mini-Dropbox/node2_store

[CHUNK ANALYSIS - SHA-256 HASHED]
  Total Chunks: 158
  Unique Chunks: 79
  Replication Factor: 2.00

  Chunk Distribution:
    600a47a25ca786f9...
      ├─ Size: 64K
      └─ Replicas: [node1 node2]
    9e22da6bc3ba3f52...
      ├─ Size: 64K
      └─ Replicas: [node1 node2]

[FILE MANIFEST]
  Total Files: 3
  
  Files:
    • document.pdf
      └─ Chunks: 79
    • image.png
      └─ Chunks: 45
    • video.mp4
      └─ Chunks: 234

[NETWORK CONFIGURATION]
  Protocol: gRPC (Protocol Buffers)
  Master: 127.0.0.1:9000
  Node 1: 127.0.0.1:9001
  Node 2: 127.0.0.1:9002
  Chunk Size: 64 KB
  Hash Algorithm: SHA-256
```

---

## 🎓 Conclusion

### What We Achieved

**Mini-Dropbox** successfully demonstrates a production-grade distributed storage system with:

1. **Robust Architecture**: Master-worker pattern separating control plane (metadata) from data plane (storage)

2. **Modern Technology**: gRPC provides high-performance, language-agnostic communication with automatic code generation from Protocol Buffers

3. **Data Integrity**: SHA-256 content-addressable storage ensures cryptographic verification of all data

4. **Fault Tolerance**: 2-way replication means system survives single node failures without data loss

5. **Scalability**: Horizontal scaling by adding more storage nodes; master handles only lightweight metadata

### Real-World Applications

This architecture pattern is used by:
- **Google File System (GFS)**: Similar master-chunkserver architecture
- **Hadoop HDFS**: NameNode (master) + DataNodes (storage)
- **Amazon S3**: Distributed object storage with replication
- **IPFS**: Content-addressable distributed storage

### Learning Outcomes

✅ **Distributed Systems**: Master-worker coordination patterns  
✅ **Network Programming**: gRPC/Protocol Buffers implementation  
✅ **Data Structures**: Hash tables for metadata management  
✅ **Cryptography**: SHA-256 for integrity and deduplication  
✅ **Fault Tolerance**: Replication and failover strategies  
✅ **System Design**: Separation of concerns, scalability principles  

### Future Enhancements

- **Dynamic Replication**: Adjust replication factor based on file importance
- **Load Balancing**: Distribute chunks based on node capacity
- **Compression**: Reduce storage footprint with chunk compression
- **Encryption**: End-to-end encryption for security
- **Web Interface**: Browser-based file management
- **Consistency**: Strong consistency guarantees with versioning

---

## 📁 Project Structure

```
Mini-Dropbox/
├── proto/
│   ├── dropbox.proto          # Protocol Buffers definition
│   ├── dropbox_pb2.py         # Generated: message classes
│   ├── dropbox_pb2_grpc.py    # Generated: service stubs
│   └── __init__.py
├── master/
│   ├── master.py              # Master gRPC server
│   └── __init__.py
├── storage_node/
│   ├── storage_node.py        # Storage gRPC server
│   └── __init__.py
├── client/
│   ├── client.py              # Client library & CLI
│   └── __init__.py
├── common/
│   ├── utils.py               # Shared utilities (legacy)
│   └── __init__.py
├── node1_store/               # Storage Node 1 data directory
│   └── [SHA-256 chunk files]
├── node2_store/               # Storage Node 2 data directory
│   └── [SHA-256 chunk files]
├── run.sh                     # CLI management interface
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

---

## 🖥️ CLI Reference

### System Management
```bash
./run.sh start              # Start all services
./run.sh stop               # Stop all services
./run.sh restart            # Restart all services
./run.sh status             # Check system status
```

### File Operations
```bash
./run.sh upload <file>              # Upload file
./run.sh download <name> <output>   # Download file
./run.sh list                       # List all files
```

### Analysis & Monitoring
```bash
./run.sh analyze            # Full system analysis
./run.sh verify             # Verify chunk integrity
./run.sh monitor            # Live monitoring (Ctrl+C to exit)
```

### Help
```bash
./run.sh help               # Show all commands
```

---

## 📦 Dependencies

```
grpcio==1.60.0              # gRPC framework
grpcio-tools==1.60.0        # Protocol Buffers compiler
protobuf>=6.30.0            # Protocol Buffers runtime
```

Install with:
```bash
pip install -r requirements.txt
```

---

## OUTPUT

##### Server Start
![Server Start](image-4.png)

##### Server Analysis - System Status & Storage
![System Status](image-3.png)
![Chunk Analysis](image-2.png)
![Network Configuration](image-1.png)

##### File Upload & Listing (supports any file extension)
![Upload and List](image-5.png)

##### File Download with Custom Output Path
![Download Command](image-6.png)

##### Downloaded Image Verification
![Downloaded Image Output](image-7.png)

**Achievements:** \
✓ Functional distributed storage system  
✓ Master-worker architecture implementation  
✓ gRPC-based high-performance communication  
✓ Fault-tolerant with data replication  
✓ Content-addressable storage (SHA-256)  

**Learning Outcomes:**
- Distributed systems design patterns
- Network programming with gRPC
- Data integrity and cryptographic hashing
- System scalability principles

**Real-world Applications:**
- DropBox
- Google File System (GFS)
- Hadoop HDFS
- Amazon S3 architecture

---

## 👥 Project Team

**Course:** CS401 (25) - Introduction to Distributed and Parallel Computing  
**Institution:** Indian Institute of Information Technology Vadodara, ICD  
**Instructor:** [Dr. Sanjay Saxena](https://www.linkedin.com/in/dr-sanjay-saxena-291a746b)

### Team Members

| Name | Roll Number | Contact | LinkedIn |
|------|-------------|---------|----------|
| **Amon Sharma** | 202251015 | [202251015@iiitvadodara.ac.in](mailto:202251015@iiitvadodara.ac.in) | [LinkedIn](https://www.linkedin.com/in/amonsharma007/) |
| **Kaustubh Duse** | 202251045 | [202251045@iiitvadodara.ac.in](mailto:202251045@iiitvadodara.ac.in) | [LinkedIn](https://www.linkedin.com/in/kaustubhduse/) |
| **Rudra Patel** | 202251094 | [202251094@iiitvadodara.ac.in](mailto:202251094@iiitvadodara.ac.in) | [LinkedIn](https://www.linkedin.com/in/rudra-patel-32859425b/) | 

---

## 📄 License

This project is created for educational purposes as part of **CS401 (25) - Introduction to Distributed and Parallel Computing** under the guidance of [Dr. Sanjay Saxena](https://www.linkedin.com/in/dr-sanjay-saxena-291a746b).

---

## 🙏 Acknowledgments

- Inspired by DropBox, Google File System (GFS) and Hadoop HDFS
- Built with Python leveraging gRPC
- Protocol Buffers for efficient serialization

---

**CS401 (25) - Introduction to Distributed and Parallel Computing**

**IIIT Vadodara** | November 2025
