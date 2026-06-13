/**
 * FlashDB.hpp  —  Bitcask-style Key-Value Storage Engine
 *
 * Architecture:
 *   - Append-only binary log for all writes (sequential I/O → blazing fast)
 *   - In-memory KeyDir (hash map) stores only {file_offset, value_size, timestamp}
 *   - O(1) single-seek reads: keydir lookup → pread at offset
 *   - CRC32 checksums on every record for crash safety
 *   - Tombstone records for deletions
 *   - Full log replay on startup to rebuild the KeyDir
 *
 * On-disk record format (little-endian):
 *   [ crc32(4) | timestamp(8) | key_sz(4) | val_sz(4) | key(key_sz) | value(val_sz) ]
 *   val_sz == 0  →  tombstone (deleted key)
 *
 * Thread Safety: shared_mutex — unlimited concurrent reads, exclusive writes.
 */

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <shared_mutex>
#include <optional>
#include <filesystem>
#include <vector>
#include <stdexcept>

namespace flashdb {

// ── On-Disk Record Header ──────────────────────────────────────────────────
#pragma pack(push, 1)
struct RecordHeader {
    uint32_t crc32;        // CRC32 of (timestamp|key_sz|val_sz|key|value)
    int64_t  timestamp;    // Unix epoch microseconds
    uint32_t key_sz;       // Length of key in bytes
    uint32_t val_sz;       // Length of value in bytes (0 = tombstone)
};
#pragma pack(pop)

static constexpr uint32_t HEADER_SIZE = sizeof(RecordHeader);
static constexpr uint32_t TOMBSTONE   = 0;

// ── In-Memory KeyDir Entry ─────────────────────────────────────────────────
struct KeyDirEntry {
    uint64_t file_offset;   // Byte offset of the RecordHeader in log file
    uint32_t value_size;    // Byte length of the value
    int64_t  timestamp;     // Timestamp of the write (for compaction ordering)
};

// ── Engine Statistics ──────────────────────────────────────────────────────
struct EngineStats {
    uint64_t total_keys;       // Live keys in KeyDir
    uint64_t log_file_bytes;   // Raw log file size in bytes
    uint64_t total_reads;      // Cumulative get() calls
    uint64_t total_writes;     // Cumulative put() calls
    uint64_t total_deletes;    // Cumulative del() calls
    uint64_t compactions;      // Number of compaction cycles completed
};

// ── Storage Engine ─────────────────────────────────────────────────────────
class FlashDB {
public:
    /**
     * Open (or create) a FlashDB at `data_dir`.
     * On first open, an empty log is created.
     * On re-open, the existing log is replayed to rebuild the KeyDir.
     *
     * @throws std::runtime_error on I/O failure.
     */
    explicit FlashDB(std::filesystem::path data_dir);
    ~FlashDB();

    // Non-copyable, movable
    FlashDB(const FlashDB&)            = delete;
    FlashDB& operator=(const FlashDB&) = delete;
    FlashDB(FlashDB&&)                 = delete;
    FlashDB& operator=(FlashDB&&)      = delete;

    // ── Core API ───────────────────────────────────────────────────────────

    /**
     * Write (or overwrite) a key-value pair.
     * Appends a new record to the log and updates the KeyDir.
     * @returns true on success.
     * @throws std::runtime_error on I/O failure.
     */
    bool put(std::string_view key, std::string_view value);

    /**
     * Read the value for a key.
     * Performs a single pread() at the stored file offset.
     * @returns std::nullopt if the key does not exist.
     */
    std::optional<std::string> get(std::string_view key);

    /**
     * Delete a key by writing a tombstone record.
     * @returns true if the key existed and was deleted, false if not found.
     */
    bool del(std::string_view key);

    /**
     * Check if a key exists (no disk I/O).
     */
    bool exists(std::string_view key) const;

    /**
     * Return a snapshot list of all live keys (no disk I/O).
     */
    std::vector<std::string> keys() const;

    /**
     * Force all buffered data to disk (fdatasync).
     */
    void sync();

    /**
     * Close the engine, flushing all data. After close(), all operations
     * will throw. Idempotent.
     */
    void close();

    /**
     * Return engine statistics (thread-safe snapshot).
     */
    EngineStats stats() const;

    // ── Internal helpers exposed for Compactor ─────────────────────────────

    /** Absolute path to the active log file. */
    std::filesystem::path log_path() const;

    /**
     * Atomically swap the active log file with a compacted replacement.
     * Called by the Compactor after it has written a clean `.tmp` file.
     * Acquires a write lock, renames the file, re-opens the fd, and
     * patches all KeyDir offsets to match the new file layout.
     *
     * @param new_log      Path to the compacted log (will be renamed to log_path())
     * @param new_keydir   New KeyDir entries referencing offsets in new_log
     */
    void swapLog(const std::filesystem::path& new_log,
                 std::unordered_map<std::string, KeyDirEntry> new_keydir);

    /**
     * Acquire a shared read lock and snapshot the KeyDir.
     * Used by the Compactor to get a consistent starting point.
     */
    std::unordered_map<std::string, KeyDirEntry> snapshotKeyDir() const;

    /** Increment compaction counter (called by Compactor). */
    void recordCompaction();

private:
    friend class Compactor;

    // ── I/O primitives ─────────────────────────────────────────────────────
    void     openLog();
    void     replayLog();
    uint64_t appendRecord(std::string_view key,
                          std::string_view value,
                          int64_t          timestamp);

    // ── CRC ────────────────────────────────────────────────────────────────
    static uint32_t computeCRC(const RecordHeader& hdr,
                               std::string_view    key,
                               std::string_view    value);

    // ── State ──────────────────────────────────────────────────────────────
    std::filesystem::path data_dir_;
    std::filesystem::path log_path_;

    int                     log_fd_{-1};
    uint64_t                write_pos_{0};   // current append position

    mutable std::shared_mutex              mutex_;
    std::unordered_map<std::string, KeyDirEntry> keydir_;

    // Statistics (atomic to allow reads without write lock)
    mutable std::atomic<uint64_t> stat_reads_{0};
    mutable std::atomic<uint64_t> stat_writes_{0};
    mutable std::atomic<uint64_t> stat_deletes_{0};
    mutable std::atomic<uint64_t> stat_compactions_{0};

    bool closed_{false};
};

} // namespace flashdb
