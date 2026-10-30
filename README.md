<div align="center">

# 🔴 Redis From Scratch

**A production-inspired, multithreaded Redis server clone built in pure Java — zero external dependencies.**

[![Java](https://img.shields.io/badge/Java-24-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)](https://openjdk.org/)
[![License](https://img.shields.io/badge/License-MIT-blue?style=for-the-badge)](LICENSE)
[![Build](https://img.shields.io/badge/Build-javac-success?style=for-the-badge&logo=apache&logoColor=white)]()
[![Protocol](https://img.shields.io/badge/Protocol-RESP-red?style=for-the-badge)]()

</div>

---

## 📖 About

This project is a ground-up implementation of a Redis-compatible server written in **pure Java**, built step-by-step following the [CodeCrafters "Build Your Own Redis"](https://codecrafters.io) challenge.

It implements the official **Redis Serialization Protocol (RESP)**, full master-replica streaming replication, ACID-style transactions, multiple data structures, Pub/Sub messaging, key TTL expiration, and RDB binary snapshot persistence — all with **no external libraries**.

> **Goal**: Understand how Redis works internally by building every layer from TCP sockets to replication from scratch.

---

## ✨ Features

| Feature | Description |
|---------|-------------|
| 🔌 **TCP Server** | Raw TCP socket server on any configurable port |
| ⚡ **Concurrent Clients** | Non-blocking multi-client handling via `CompletableFuture` |
| 📡 **RESP Protocol** | Full binary-safe RESP parser and encoder |
| 🗄️ **Strings** | `SET`, `GET`, `INCR`, `EXISTS`, `DEL`, `KEYS` with TTL/PX expiry |
| 📋 **Lists** | `LPUSH`, `RPUSH`, `LRANGE` with negative-index support |
| 🗂️ **Hashes** | `HSET`, `HGET`, `HGETALL` for object-style storage |
| 🔢 **Sets** | `SADD`, `SMEMBERS`, `SISMEMBER` with automatic deduplication |
| 📣 **Pub/Sub** | Real-time channel broadcasting via `SUBSCRIBE` / `PUBLISH` |
| 💳 **Transactions** | `MULTI` / `EXEC` / `DISCARD` with per-client command queuing |
| 🔄 **Replication** | 3-step handshake, live write propagation from master → replicas |
| ⏳ **WAIT Command** | Synchronous replica acknowledgement with timeout |
| 💾 **RDB Persistence** | Binary snapshot (`SAVE`) with auto-restore on server boot |
| ⚙️ **Config CLI** | `--port`, `--dir`, `--dbfilename`, `--replicaof` flags |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Redis From Scratch                       │
│                                                             │
│  TCP Client          Main.java (Single File Architecture)   │
│  ──────────►  ┌──────────────────────────────────────────┐  │
│               │  ServerSocket (port 6379)                 │  │
│               │       │                                   │  │
│               │  CompletableFuture.runAsync()             │  │
│               │  ┌────┴───────────────────────┐           │  │
│               │  │   handleClient(socket)      │           │  │
│               │  │   ┌────────────────────┐   │           │  │
│               │  │   │  RESP Parser       │   │           │  │
│               │  │   │  (Scanner loop)    │   │           │  │
│               │  │   └────────┬───────────┘   │           │  │
│               │  │            ▼                │           │  │
│               │  │   executeCommand()          │           │  │
│               │  │   ┌──────────────────────┐ │           │  │
│               │  │   │ switch(command)       │ │           │  │
│               │  │   │  ├─ Strings           │ │           │  │
│               │  │   │  ├─ Lists             │ │           │  │
│               │  │   │  ├─ Hashes            │ │           │  │
│               │  │   │  ├─ Sets              │ │           │  │
│               │  │   │  ├─ Pub/Sub           │ │           │  │
│               │  │   │  ├─ Transactions      │ │           │  │
│               │  │   │  ├─ Replication       │ │           │  │
│               │  │   │  └─ Persistence       │ │           │  │
│               │  │   └──────────────────────┘ │           │  │
│               │  └─────────────────────────────┘           │  │
│               └──────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### In-Memory Data Stores

| Field | Type | Purpose |
|-------|------|---------|
| `database` | `Map<String, ValueEntry>` | Strings with optional TTL |
| `listDatabase` | `Map<String, Deque<String>>` | Redis Lists (LPUSH/RPUSH/LRANGE) |
| `hashDatabase` | `Map<String, Map<String, String>>` | Redis Hashes (HSET/HGET/HGETALL) |
| `setDatabase` | `Map<String, Set<String>>` | Redis Sets (SADD/SMEMBERS/SISMEMBER) |
| `channelSubscribers` | `Map<String, Set<OutputStream>>` | Pub/Sub subscriber registry |
| `replicaStreams` | `List<OutputStream>` | Connected replica sockets |

---

## 🚀 Getting Started

### Prerequisites
- **Java 17+** (tested on Java 24)
- **PowerShell** (Windows) or adapt `run.ps1` to a shell script

### Clone & Run

```powershell
git clone https://github.com/priyanshu0508/Redis-From-Scratch.git
cd Redis-From-Scratch

# Start the master server on port 6379
.\run.ps1
```

### Start a Replica

```powershell
# In a second terminal — replica on port 6380
.\run.ps1 --port 6380 --replicaof "localhost 6379"
```

### Custom Storage Path

```powershell
.\run.ps1 --dir C:\data\redis --dbfilename mydb.rdb
```

### Manual Compilation

```powershell
javac -d bin src/Main.java
java -cp bin Main --port 6379
```

---

## 📋 Supported Commands

### 🔤 Strings & Keys

```bash
SET planet Earth          # Store a string value
SET counter 0 PX 5000     # Store with 5-second TTL (milliseconds)
GET planet                # → "Earth"
INCR counter              # Atomic increment → 1
EXISTS planet             # → 1 (exists) or 0 (not found)
DEL planet counter        # Delete one or more keys → count deleted
KEYS *                    # List all active keys
```

### 📋 Lists

```bash
RPUSH tasks "task_one" "task_two"   # Append to tail → length
LPUSH tasks "task_zero"             # Prepend to head → length
LRANGE tasks 0 -1                   # Get all items
LRANGE tasks 0 1                    # Get first 2 items
```

### 🗂️ Hashes

```bash
HSET user:1 name Alice age 28 role engineer   # Set multiple fields → new fields count
HGET user:1 name                              # → "Alice"
HGETALL user:1                                # → [name, Alice, age, 28, role, engineer]
```

### 🔢 Sets

```bash
SADD fruits apple banana apple mango   # Add members (dedup) → 3 added
SMEMBERS fruits                        # → [apple, banana, mango]
SISMEMBER fruits apple                 # → 1 (member exists)
SISMEMBER fruits grape                 # → 0 (not a member)
```

### 📣 Pub/Sub

```bash
# Terminal 1 — Subscriber
SUBSCRIBE news sports

# Terminal 2 — Publisher
PUBLISH news "Breaking: Redis PubSub works!"   # → 1 (delivered to 1 subscriber)
```

### 💳 Transactions

```bash
MULTI              # Begin transaction
SET key1 "a"       # Queued
INCR counter       # Queued
SET key2 "b"       # Queued
EXEC               # Execute all atomically → [+OK, :1, +OK]
DISCARD            # Or discard the queue
```

### 🔄 Replication

```bash
INFO replication          # Shows role, master_replid, master_repl_offset
WAIT 1 500                # Block until 1 replica ACKs, or 500ms timeout
```

### 💾 Persistence

```bash
SAVE    # Serialize all data to dump.rdb → +OK
```

---

## 🔄 Replication Architecture

```
Master (port 6379)                 Replica (port 6380)
─────────────────                  ───────────────────
                    ← PING
                    PONG →
                    ← REPLCONF listening-port 6380
                    OK →
                    ← REPLCONF capa psync2
                    OK →
                    ← PSYNC ? -1
                    FULLRESYNC <replid> 0 →
                    ← RDB Snapshot (future)

[Live Propagation]
SET foo bar   ──► Propagate RESP array ──► replica applies SET foo bar
HSET ...      ──► Propagate            ──► replica applies HSET ...
SADD ...      ──► Propagate            ──► replica applies SADD ...
```

---

## 💾 RDB Persistence Format

The snapshot file uses a custom binary format inspired by Redis's `REDIS0009`:

```
[9 bytes]  Magic header: "REDIS0009"
[4 bytes]  String count (int)
  per string:
    [UTF]     key
    [UTF]     value
    [bool]    hasExpiry
    [long?]   expiresAt (if hasExpiry)
[4 bytes]  List count (int)
  per list:
    [UTF]     key
    [int]     item count
    [UTF...]  items
[4 bytes]  Hash count (int)
  per hash:
    [UTF]     key
    [int]     field count
    [UTF,UTF] field, value pairs
[4 bytes]  Set count (int)
  per set:
    [UTF]     key
    [int]     member count
    [UTF...]  members
[1 byte]   0xFF EOF marker
```

---

## 📁 Project Structure

```
Redis-From-Scratch/
├── src/
│   └── Main.java          # Entire server (~830 lines, zero dependencies)
├── bin/
│   └── Main.class         # Compiled bytecode (git-ignored)
├── run.ps1                # PowerShell build + run script
├── .gitignore
└── README.md
```

---

## 🗺️ Build Roadmap

| Step | Feature | Status |
|------|---------|--------|
| 1 | Bind TCP socket to port 6379 | ✅ |
| 2 | `PING` → `+PONG` | ✅ |
| 3 | Multiple commands on same connection | ✅ |
| 4 | Concurrent clients with `CompletableFuture` | ✅ |
| 5 | `ECHO` command | ✅ |
| 6 | `SET` and `GET` | ✅ |
| 7 | Key expiry with `PX` TTL | ✅ |
| 8 | `EXISTS`, `DEL`, `KEYS *` | ✅ |
| 9 | `CONFIG GET` + CLI args (`--dir`, `--dbfilename`) | ✅ |
| 10 | `MULTI` / `EXEC` / `DISCARD` Transactions + `INCR` | ✅ |
| 11 | `INFO replication` + role config | ✅ |
| 12 | Replica handshake (`PING`, `REPLCONF`, `PSYNC`) | ✅ |
| 13 | Live master→replica command propagation | ✅ |
| 14 | Redis Lists (`LPUSH`, `RPUSH`, `LRANGE`) | ✅ |
| 15 | Redis Hashes (`HSET`, `HGET`, `HGETALL`) | ✅ |
| 16 | RDB Persistence (`SAVE` + auto-restore on boot) | ✅ |
| 17 | Pub/Sub (`SUBSCRIBE`, `PUBLISH`) | ✅ |
| 18 | Redis Sets (`SADD`, `SMEMBERS`, `SISMEMBER`) | ✅ |
| 19 | `WAIT` command (sync replication ACK) | ✅ |

---

## 🧠 Key Design Decisions

- **Single-file architecture**: All ~830 lines live in `Main.java` — no Maven, no Gradle, no frameworks. Pure `javac`.
- **RESP parser**: Uses `Scanner.nextLine()` in a `while(hasNextLine())` loop — simple, robust, zero allocations beyond Strings.
- **Thread safety**: All data stores use `ConcurrentHashMap` / `ConcurrentLinkedDeque` / `ConcurrentHashMap.newKeySet()`. No explicit `synchronized` blocks on hot paths.
- **Replica persistence**: Replica socket is kept alive after `PSYNC` using a `continue` in the parse loop — no reconnect needed for propagation.
- **Pub/Sub isolation**: Each subscriber's `OutputStream` is stored per-channel. `PUBLISH` writes directly to raw sockets, bypassing the normal request/response loop.
