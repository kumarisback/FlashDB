# FlashDB — Bitcask-style Key-Value Engine

> A production-grade, append-only, single-seek KV storage engine modeled after
> **Riak's Bitcask** and the **Redis/RocksDB WAL** design. Built in **C++20**.

---

## Table of Contents

1. [The Big Idea](#the-big-idea)
2. [Architecture](#architecture)
3. [How Data Flows](#how-data-flows)
   - [Write (PUT)](#write-put)
   - [Read (GET)](#read-get)
   - [Delete (DEL)](#delete-del)
   - [Compaction (GC)](#compaction-gc)
4. [Project Structure](#project-structure)
5. [Build](#build)
6. [Run](#run)
7. [All Commands](#all-commands)
8. [Demo Scripts](#demo-scripts)
9. [Benchmark](#benchmark)
10. [On-Disk Binary Format](#on-disk-binary-format)
11. [Enterprise Features](#enterprise-features)

---

## The Big Idea

Most databases store data in complex tree structures on disk (B-Trees).
FlashDB uses a much simpler and faster trick: **the Append-Only Log**.

```
NEVER overwrite.  ALWAYS append.

flashdb.log (on disk):
┌──────────┬──────────┬──────────┬──────────┬──────────┐
│SET foo=1 │SET bar=2 │SET foo=99│DEL bar   │SET x=5   │
│  ↑        ↑          ↑          ↑          ↑          │
│  Every write just adds to the END.  Sequential I/O.   │
└──────────────────────────────────────────────────────-┘
```

- **Writes** → append a binary record to the end of `flashdb.log`. Sequential disk writes are the fastest possible I/O.
- **Reads** → look up the exact byte offset in an **in-memory hash map** (the KeyDir), then do ONE `pread()` syscall. O(1), single disk seek.
- **Stale data** → cleaned up by the **Compactor** running on a background thread.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     YOU (Client)                        │
│         redis-cli -p 6399 SET name Alice                │
└───────────────────────┬─────────────────────────────────┘
                        │  TCP packet (port 6399)
                        │  Text: "SET name Alice\r\n"
                        ▼
┌─────────────────────────────────────────────────────────┐
│              LAYER 1: Server.cpp                        │
│                                                         │
│  kqueue (macOS) / epoll (Linux) event loop              │
│  Watches ALL client sockets simultaneously              │
│  Data arrives → parse "SET name Alice" → tokens         │
│  → call engine.put("name", "Alice")                     │
│  → send "+OK\r\n" back to client                        │
└───────────────────────┬─────────────────────────────────┘
                        │  Direct function call (in-process)
                        ▼
┌─────────────────────────────────────────────────────────┐
│              LAYER 2: FlashDB.cpp                       │
│                                                         │
│  put("name", "Alice"):                                  │
│    1. Build binary record [CRC|ts|4|5|name|Alice]       │
│    2. writev() → appends to flashdb.log                 │
│    3. keydir_["name"] = {offset=0, size=5}              │
│                                                         │
│  get("name"):                                           │
│    1. keydir_["name"] → {offset=0, size=5}              │
│    2. pread(fd, buf, 5, offset) → "Alice"               │
│    3. Return "Alice"  ← ONE disk seek, no scanning!     │
│                                                         │
│  IN MEMORY (KeyDir):    ON DISK (flashdb.log):          │
│  ┌──────────────────┐   ┌─────────────────────────┐     │
│  │"name"→ offset: 0 │   │[CRC][ts][4][5]nameAlice │     │
│  │"age" → offset:57 │   │[CRC][ts][3][2]age 42    │     │
│  └──────────────────┘   └─────────────────────────┘     │
└───────────────────────┬─────────────────────────────────┘
                        │  Background thread (every 60s)
                        ▼
┌─────────────────────────────────────────────────────────┐
│              LAYER 3: Compactor.cpp                     │
│                                                         │
│  1. Snapshot: copy current keydir_ (live keys only)     │
│  2. For each live key → read its record from OLD file   │
│  3. Write ONLY live records to flashdb.log.tmp          │
│  4. rename(tmp → flashdb.log)  ← atomic! Zero loss     │
│  5. Patch keydir_ with new offsets                      │
└─────────────────────────────────────────────────────────┘
```

---

## How Data Flows

### Write (PUT)

```
redis-cli -p 6399 SET user:1 Alice
```

**1. Server parses TCP bytes**
```
Raw bytes:  "SET user:1 Alice\r\n"
Tokens:     ["SET", "user:1", "Alice"]
Action:     engine_.put("user:1", "Alice")
```

**2. FlashDB builds a binary record**
```
RecordHeader {
    crc32     = 0xA7B3C1D2   ← CRC32 over entire record (crash safety)
    timestamp = 1718132534   ← Unix microseconds
    key_sz    = 6            ← length of "user:1"
    val_sz    = 5            ← length of "Alice"
}

Bytes appended to flashdb.log (31 bytes total):
┌──────────┬─────────────────┬────────┬────────┬────────┬───────┐
│ CRC (4B) │ Timestamp (8B)  │ ksz(4B)│ vsz(4B)│ key(6B)│val(5B)│
│ A7B3C1D2 │ 000000662A1FB6  │ 000006 │ 000005 │user:1  │ Alice │
└──────────┴─────────────────┴────────┴────────┴────────┴───────┘
```

**3. KeyDir updated in memory**
```cpp
keydir_["user:1"] = { file_offset=0, value_size=5, timestamp=... };
```

**4. Server responds:** `+OK\r\n`

---

### Read (GET)

```
redis-cli -p 6399 GET user:1
```

**1. KeyDir lookup — ZERO disk I/O**
```
keydir_.find("user:1") → { file_offset=0, value_size=5 }
```

**2. ONE precise disk read**
```
pread(
    log_fd,
    output_buf,
    5,                              // read exactly 5 bytes
    0 + HEADER_SIZE(20) + 6(key)   // jump directly to value
)
→ "Alice"
```

> This is the O(1) single-seek design. The disk arm moves to ONE location
> and reads exactly the bytes we need. No scanning. No B-tree traversal.

**3. Server responds:** `$5\r\nAlice\r\n`

---

### Delete (DEL)

FlashDB never erases bytes from the file. Instead it appends a **tombstone**:

```
DEL user:1
→ Appends: [CRC][timestamp][6][0][user:1]
                               ↑
                         val_sz=0 signals "deleted"
→ Removes "user:1" from in-memory keydir_
```

- Next `GET user:1` → not in keydir_ → returns nil.
- On restart, log replay encounters the tombstone → erases from keydir_.
- Compactor skips tombstones → space is reclaimed automatically.

---

### Compaction (GC)

The Compactor removes stale (overwritten/deleted) records from disk.

**Before compaction** — `foo` was overwritten 5 times:
```
flashdb.log:
┌──────────┬──────────┬──────────┬──────────┬──────────┬──────────┐
│ foo=hello│ foo=world│ foo=test │ foo=baz  │ bar=xyz  │ foo=FINAL│
│  DEAD    │  DEAD    │  DEAD    │  DEAD    │  LIVE    │  LIVE    │
└──────────┴──────────┴──────────┴──────────┴──────────┴──────────┘
File size: 300 bytes
```

**After compaction:**
```
flashdb.log:
┌──────────┬──────────┐
│ bar=xyz  │ foo=FINAL│   ← only the 2 live records
└──────────┴──────────┘
File size: 100 bytes  (67% reclaimed!)
```

`rename(tmp → flashdb.log)` is **atomic** on POSIX. The database keeps
serving requests the entire time. Write lock is held for < 1ms.

---

## Project Structure

```
FlashDB/
├── CMakeLists.txt              Build system (C++20, Release -O3, Debug+ASan)
├── README.md                   This file
├── include/
│   ├── FlashDB.hpp             Storage engine — KeyDir, binary format, API
│   ├── Compactor.hpp           Background GC — interface
│   └── Server.hpp              Async TCP server — kqueue/epoll, RESP protocol
├── src/
│   ├── FlashDB.cpp             Core engine: append, pread, CRC, log replay
│   ├── Compactor.cpp           Background thread, snapshot, atomic log swap
│   └── Server.cpp              Event loop, connection handling, RESP parser
├── benchmarks/
│   └── stress_test.cpp         Throughput suite: 6 benchmark phases
└── main.cpp                    Entry point: CLI args, signal handling, wiring
```

---

## Build

```bash
# Clone / navigate to the project
cd "/Users/arunkumar/Workspace/System Internal/DB/FlashDB"

# Configure (Release build — maximum performance)
cmake -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=/usr/bin/clang++

# Build both targets
cmake --build build -j8
```

Two executables are produced:
| Executable | Purpose |
|------------|---------|
| `build/FlashDB` | The database server |
| `build/benchmark` | Performance benchmark (standalone) |

> **Debug build** (with AddressSanitizer + UBSan):
> ```bash
> cmake -B build-debug -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_COMPILER=/usr/bin/clang++
> cmake --build build-debug -j8
> ```

---

## Run

### Terminal 1 — Start the server

```bash
./build/FlashDB --port 6399 --data ./mydata
```

**CLI options:**

| Flag | Default | Description |
|------|---------|-------------|
| `--port <n>` | `6399` | TCP port to listen on |
| `--data <dir>` | `./data` | Directory to store `flashdb.log` |
| `--compact-interval <s>` | `60` | Seconds between GC runs |
| `--compact-threshold <bytes>` | `67108864` | GC triggers when log exceeds this size (64 MB) |
| `--workers <n>` | `hw_concurrency` | Thread pool size |

**Expected startup output:**
```
  _____ _           _     ____  ____
 |  ___| | __ _ ___| |__ |  _ \| __ )
 ...

  Port    : 6399
  Data    : ./mydata
  Compact : every 60s | threshold 64 MB
  Protocol: RESP (redis-cli compatible)

[FlashDB] Engine opened at ./mydata
[FlashDB] Recovered 0 keys from 0 bytes on disk
[Compactor] Background GC started
[Server] Listening on 0.0.0.0:6399
[Server] Worker threads: 10
[FlashDB] Ready. Connect with: redis-cli -p 6399
```

### Terminal 2 — Connect

```bash
# Option A: redis-cli (recommended)
redis-cli -p 6399

# Option B: netcat (no redis-cli needed)
nc localhost 6399

# Option C: one-liner
redis-cli -p 6399 SET key value
```

---

## All Commands

| Command | Example | Response | What it does |
|---------|---------|----------|--------------|
| `PING` | `PING` | `+PONG` | Connection check |
| `SET key value` | `SET name Alice` | `+OK` | Write a key-value pair |
| `GET key` | `GET name` | `$5 Alice` or `$-1` | Read a value (nil if missing) |
| `DEL key` | `DEL name` | `:1` or `:0` | Delete a key (writes tombstone) |
| `EXISTS key` | `EXISTS name` | `:1` or `:0` | Check if key exists (no disk I/O) |
| `KEYS` | `KEYS` | `*2 $4 name ...` | List all live keys (no disk I/O) |
| `STATS` | `STATS` | Bulk string | Engine + server statistics |
| `COMPACT` | `COMPACT` | `+OK` | Trigger immediate GC |
| `QUIT` | `QUIT` | `+BYE` | Close connection |

**Full interactive session:**
```
127.0.0.1:6399> PING
PONG

127.0.0.1:6399> SET name Alice
OK

127.0.0.1:6399> SET age 25
OK

127.0.0.1:6399> SET city Mumbai
OK

127.0.0.1:6399> GET name
"Alice"

127.0.0.1:6399> GET ghost
(nil)

127.0.0.1:6399> EXISTS name
(integer) 1

127.0.0.1:6399> SET name Bob
OK

127.0.0.1:6399> GET name
"Bob"

127.0.0.1:6399> DEL name
(integer) 1

127.0.0.1:6399> GET name
(nil)

127.0.0.1:6399> KEYS
1) "age"
2) "city"

127.0.0.1:6399> STATS
"# FlashDB Stats
total_keys:2
log_file_bytes:243
total_reads:4
total_writes:5
total_deletes:1
compactions:0
connections_total:1
connections_active:1
commands_processed:12"

127.0.0.1:6399> COMPACT
OK

127.0.0.1:6399> QUIT
BYE
```

---

## Demo Scripts

### Demo 1 — Persistence (Data survives restart)

```bash
# Write some data
redis-cli -p 6399 SET company Google
redis-cli -p 6399 SET role "Software Engineer"
redis-cli -p 6399 SET city Bangalore

# Kill the server (Ctrl+C), then restart:
./build/FlashDB --port 6399 --data ./mydata
# → [FlashDB] Recovered 3 keys from XXX bytes on disk

# Data still there!
redis-cli -p 6399 KEYS
redis-cli -p 6399 GET company
# "Google"
```

> **How it works**: On startup, the engine reads `flashdb.log` from byte 0,
> verifies the CRC on each record, and rebuilds the in-memory KeyDir in milliseconds.

---

### Demo 2 — Compaction (Space Reclamation)

```bash
# Overwrite the same key 50 times
for i in $(seq 1 50); do redis-cli -p 6399 SET counter $i; done

# 50 records on disk for 1 live value
redis-cli -p 6399 STATS
# log_file_bytes: ~1350

# Trigger GC
redis-cli -p 6399 COMPACT

# Only 1 record remains
redis-cli -p 6399 STATS
# log_file_bytes: ~27  (98% reclaimed!)
```

> **How it works**: The Compactor runs on a background thread. It writes only
> the current live value to a `.tmp` file, then calls `rename()` — which is
> atomic at the OS level. Zero downtime. Zero data loss.

---

### Demo 3 — CRC Crash Safety

```bash
# Stop the server (Ctrl+C)
# Corrupt the log file manually
printf '\xDE\xAD\xBE\xEF' | dd of=./mydata/flashdb.log bs=1 seek=0 conv=notrunc 2>/dev/null

# Restart
./build/FlashDB --port 6399 --data ./mydata
# → [FlashDB] CRC mismatch at offset 0, stopping replay.
# → [FlashDB] Recovered 0 keys from 0 bytes on disk
```

> **How it works**: Every record carries a CRC32 checksum. If the process
> crashes mid-write, the partial record fails its checksum on replay and is
> safely truncated — no corrupt data is ever served.

---

### Demo 4 — View Raw Binary Format on Disk

```bash
# After writing some data:
hexdump -C ./mydata/flashdb.log | head -5
```

**Annotated output:**
```
Offset  Bytes                           Meaning
──────  ──────────────────────────────  ──────────────────────────
000000  a7 b3 c1 d2                     CRC32 checksum (4 bytes)
000004  62 61 29 05 03 54 06 00         Timestamp in microseconds (8 bytes)
00000c  04 00 00 00                     key_sz = 4 → key is "name"
000010  05 00 00 00                     val_sz = 5 → value is "Alice"
000014  6e 61 6d 65                     "name" in ASCII
000018  41 6c 69 63 65                  "Alice" in ASCII
```

> This is the exact same format used by **Riak's Bitcask** storage engine.
> It is self-describing — each record carries its own size and checksum.

---

## Benchmark

```bash
./build/benchmark 1000000   # 1 million operations
```

**Results on Apple Silicon (M-series):**

| Benchmark | Throughput | Latency |
|-----------|-----------|---------|
| Sequential PUT | **574,560 ops/s** | 1.74 µs |
| Sequential GET | **1,540,622 ops/s** | 0.65 µs |
| Random GET | **1,064,763 ops/s** | 0.94 µs |
| Overwrite PUT | **536,847 ops/s** | 1.86 µs |
| Mixed 80% Read / 20% Write | **885,747 ops/s** | 1.13 µs |
| DELETE (10% of keys) | **513,713 ops/s** | 1.95 µs |

**Compaction** (500K records, 73 MB log):
```
Compaction time : ~1,000 ms
Space saved     : 73 MB  (57% reclaimed)
```

---

## On-Disk Binary Format

Every record is a flat binary structure (little-endian, no padding):

```
┌──────────┬──────────────────┬──────────┬──────────┬──────────────┬────────────────┐
│ CRC32    │   Timestamp      │  key_sz  │  val_sz  │     key      │     value      │
│  4 bytes │    8 bytes       │  4 bytes │  4 bytes │  key_sz bytes│  val_sz bytes  │
└──────────┴──────────────────┴──────────┴──────────┴──────────────┴────────────────┘
```

| Field | Size | Description |
|-------|------|-------------|
| `crc32` | 4 B | CRC32 of (timestamp + key_sz + val_sz + key + value) |
| `timestamp` | 8 B | Unix epoch in microseconds |
| `key_sz` | 4 B | Length of key in bytes |
| `val_sz` | 4 B | Length of value in bytes. **`0` = tombstone (deleted)** |
| `key` | key_sz B | Key bytes |
| `value` | val_sz B | Value bytes |

**Tombstone**: a record with `val_sz = 0` signals that the key was deleted.

---

## Enterprise Features

| Feature | Implementation |
|---------|---------------|
| **Append-only writes** | `writev()` — one syscall, sequential I/O |
| **O(1) reads** | `pread()` at stored offset, no scan |
| **CRC32 integrity** | Every record checksummed on write + replay |
| **Crash recovery** | Full log replay on startup, truncates corrupt tail |
| **Concurrent reads** | `std::shared_mutex` — unlimited readers |
| **Background GC** | `std::thread` + `condition_variable`, atomic swap |
| **Async networking** | `kqueue` (macOS) / `epoll` (Linux), edge-triggered |
| **10K connections** | Non-blocking sockets, per-conn read/write buffers |
| **Redis-compatible** | RESP protocol — works with any `redis-cli` client |
| **Graceful shutdown** | `SIGINT`/`SIGTERM` → drain → fsync → close |

---

## Stop the Server

```bash
# In the server terminal: Ctrl+C
# OR:
pkill FlashDB
```

```
[FlashDB] Received signal 15, shutting down...
[Server] Stopped.
[FlashDB] Goodbye.
```

---

## References

- [Bitcask: A Log-Structured Hash Table for Fast Key/Value Data](https://riak.com/assets/bitcask-intro.pdf) — the original Riak paper this engine is based on
- [The Log: What every software engineer should know](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying) — LinkedIn Engineering
- Redis RESP Protocol: https://redis.io/docs/reference/protocol-spec/
