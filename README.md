# Redis From Scratch (Java)

A production-grade, multithreaded, in-memory key-value database and data structure server built from scratch in pure Java (zero external dependencies). Implements the official **Redis Serialization Protocol (RESP)**, persistent live master-replica streaming replication, transactions, key TTL expiration, and RDB disk persistence.

---

## 🚀 Features & Architecture

- **Protocol**: Full binary-safe RESP implementation supporting Simple Strings (`+`), Errors (`-`), Integers (`:`), Bulk Strings (`$`), Null Bulk Strings (`$-1`), and Arrays (`*`).
- **Concurrency**: Asynchronous non-blocking client connection handling using Java Virtual/ThreadPool workers via `CompletableFuture`.
- **In-Memory Store**: Thread-safe concurrent storage supporting TTL expiration with passive lazy eviction.
- **Data Structures**:
  - **Strings**: `SET`, `GET`, `INCR`, `EXISTS`, `DEL`, `KEYS *`
  - **Lists**: `LPUSH`, `RPUSH`, `LRANGE`
  - **Hashes**: `HSET`, `HGET`, `HGETALL`
- **Pub/Sub Messaging**: Real-time event broadcasting via `SUBSCRIBE` and `PUBLISH`.
- **Transactions**: `MULTI`, `EXEC`, `DISCARD` with per-client command queuing.
- **Distributed Replication**:
  - Role management (`master` vs `slave`) via `INFO replication`.
  - 3-step replica handshake (`PING`, `REPLCONF`, `PSYNC`).
  - Real-time live master-to-replica write command propagation over persistent TCP streams.
- **Durability (RDB Persistence)**:
  - `SAVE` serializes in-memory databases to disk using binary `REDIS0009` snapshot format.
  - Automatic database restoration on server startup.

---

## 🛠️ How to Run

### 1. Start as Master (Default Port 6379)
```powershell
.\run.ps1
```

### 2. Start as Replica (e.g. Port 6380 pointing to Master)
```powershell
.\run.ps1 --port 6380 --replicaof "localhost 6379"
```

### 3. Custom Storage Location
```powershell
.\run.ps1 --dir /path/to/data --dbfilename my_dump.rdb
```

---

## 📋 Supported Commands

| Category | Commands |
|----------|----------|
| **Core & Debugging** | `PING`, `ECHO <msg>`, `CONFIG GET <dir\|dbfilename>` |
| **Strings & Expiry** | `SET <key> <val> [PX <ms>]`, `GET <key>`, `INCR <key>` |
| **Key Management** | `EXISTS <key>`, `DEL <key ...>`, `KEYS *` |
| **Lists** | `LPUSH <key> <val ...>`, `RPUSH <key> <val ...>`, `LRANGE <key> <start> <stop>` |
| **Hashes** | `HSET <key> <f1> <v1> ...`, `HGET <key> <f>`, `HGETALL <key>` |
| **Pub/Sub** | `SUBSCRIBE <channel ...>`, `PUBLISH <channel> <message>` |
| **Transactions** | `MULTI`, `EXEC`, `DISCARD` |
| **Replication** | `INFO replication`, `REPLCONF`, `PSYNC` |
| **Persistence** | `SAVE` |
