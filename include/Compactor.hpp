/**
 * Compactor.hpp  —  Background Garbage Collection for FlashDB
 *
 * The Problem:
 *   Bitcask is append-only. Every update to "key_1" writes a brand-new
 *   record. After 1 million updates, the log contains 1 million stale
 *   entries for a single key, wasting gigabytes of disk space.
 *
 * The Solution — Log Compaction:
 *   A background thread wakes up periodically (or when the log exceeds a
 *   configured size threshold). It performs the following steps:
 *
 *   1. Snapshot: Capture a consistent snapshot of the KeyDir — this tells
 *      us the *current* offset for each live key.
 *
 *   2. Scan & Filter: Walk the log sequentially (fast sequential I/O).
 *      For each record encountered, check if its offset matches the
 *      snapshot. If yes → live record; if no → stale, skip it.
 *
 *   3. Write Compacted File: Stream only live records into a fresh
 *      `.tmp` file. This produces a dense, zero-waste log.
 *
 *   4. Atomic Swap: Call engine.swapLog() which:
 *      - Holds the engine write lock only during rename() + reopen()
 *      - Uses POSIX rename() — atomic on all major filesystems
 *      - Patches the in-memory KeyDir to reference new offsets
 *
 * Guarantees:
 *   - Zero data loss: the original log is only removed after a
 *     successful rename.
 *   - Minimal engine pause: the write lock is held for < 1 ms.
 *   - Only one compaction runs at a time (mutex-guarded).
 *   - Graceful shutdown via stop() which can be called from any thread.
 */

#pragma once

#include "FlashDB.hpp"

#include <chrono>
#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <filesystem>

namespace flashdb {

struct CompactionStats {
    uint64_t runs_completed{0};
    uint64_t bytes_reclaimed{0};  // Bytes freed in last compaction
    uint64_t records_removed{0};  // Stale records eliminated
    std::chrono::steady_clock::time_point last_run;
};

class Compactor {
public:
    /**
     * Attach a Compactor to a running FlashDB engine.
     *
     * @param engine        Reference to the storage engine (must outlive this)
     * @param interval      How often to check for compaction triggers
     * @param size_threshold Compact when log file exceeds this many bytes
     *                      (set to 0 to compact purely on interval)
     */
    explicit Compactor(FlashDB&               engine,
                       std::chrono::seconds   interval       = std::chrono::seconds(60),
                       uint64_t               size_threshold = 64ULL * 1024 * 1024 /* 64 MB */);

    ~Compactor();

    // Non-copyable, non-movable (holds a background thread)
    Compactor(const Compactor&)            = delete;
    Compactor& operator=(const Compactor&) = delete;

    /**
     * Start the background compaction thread.
     * Safe to call multiple times (idempotent).
     */
    void start();

    /**
     * Signal the background thread to stop and wait for it to exit.
     * Called automatically by the destructor.
     */
    void stop();

    /**
     * Trigger an immediate compaction (runs synchronously in the calling
     * thread, bypassing the interval). Returns false if a compaction is
     * already running.
     */
    bool compact_now();

    /**
     * Return a snapshot of compaction statistics.
     */
    CompactionStats stats() const;

    /**
     * Register a callback invoked after each successful compaction.
     * Called from the background thread — keep it fast.
     */
    void onCompactionComplete(std::function<void(const CompactionStats&)> cb);

private:
    void   backgroundLoop();
    bool   shouldCompact() const;
    bool   runCompaction();       // Core compaction logic (returns true on success)

    FlashDB&                engine_;
    std::chrono::seconds    interval_;
    uint64_t                size_threshold_;

    std::thread             worker_;
    std::mutex              cv_mutex_;
    std::condition_variable cv_;
    std::atomic<bool>       running_{false};
    std::mutex              compact_mutex_;  // Prevents concurrent compactions

    mutable std::mutex      stats_mutex_;
    CompactionStats         stats_;

    std::function<void(const CompactionStats&)> on_complete_;
};

} // namespace flashdb
