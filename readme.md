# Rust KV Store

A simple, persistent, log-structured key-value store written in Rust.

This project implements a basic key-value database from scratch, inspired by the design of **Bitcask**.  
It features a TCP server that accepts `GET`, `SET`, and `DELETE` commands — similar to Redis.

---

## 🚀 Features

- **Persistent Storage**: All data is written to disk in log files (segments).
- **Log-Structured Design**: All writes are appends to an active log file, ensuring fast write performance. Updates and deletes are handled by appending new entries.
- **In-Memory Index**: A `HashMap` serves as an in-memory "key directory," mapping keys to the exact location of their latest value on disk for fast read access.
- **Segment Rollover**: The active log file is rolled over to a new file once it reaches a configurable maximum size, splitting the data into manageable segments.
- **Startup Reloading**: On startup, the server rebuilds the in-memory index by scanning the segment files, ensuring data is not lost between restarts.
- **TCP Server**: A simple multi-threaded TCP server listens on `127.0.0.1:6379` to handle client connections.

---

## 🛠️ How It Works

The store is built around a few core components:

### **KVStore**
The main struct that orchestrates the `Segments` and the `KeyDirectory`.  
It handles the public-facing `put`, `get`, and `delete` operations.

### **KeyDirectory**
An in-memory `HashMap` that acts as the index.  
It stores keys and maps them to an `AppendEntryResponse`, which contains the `file_id` and `offset` where the value can be found.

### **Segments**
Manages all the log files.  
It holds one `active_segment` for new writes and a collection of `inactive_segments` for old data.  
It is responsible for segment rollover.

### **Segment**
Represents a single data file on disk (e.g., `1678886400_segment.data`).  
It uses a `Store` to append and read raw bytes.

### **Store**
A low-level wrapper around a file that provides basic append and read capabilities.

### **Entry**
The unit of data that is serialized and written to the log.  
It contains the key, value, timestamp, and a tombstone marker to indicate if an entry is deleted.

---

## 📝 Write (Put) Operation

1. A `PUT` command is received.
2. The `KVStore` passes the key and value to the `Segments` manager.
3. `Segments` serializes them into an `Entry` and appends it to the active segment.
4. The `Store` writes the bytes to the end of the file.
5. The file offset and file ID are returned to the `KVStore`, which updates the `KeyDirectory` with the new location for that key.

---

## 🔍 Read (Get) Operation

1. A `GET` command is received.
2. The `KVStore` looks up the key in the `KeyDirectory`.
3. If the key exists, the directory returns the `file_id`, `offset`, and length of the data.
4. `KVStore` instructs `Segments` to read from the specified file (active or inactive) at that exact offset.
5. The `Store` seeks to the offset, reads the bytes, and returns them.
6. The `Entry` is decoded, and the value is returned to the client.

---

## 🖥️ Running the Server

You can run the server using Cargo:

```bash
cargo run
