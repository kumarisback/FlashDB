/**
 * Compactor.cpp  —  Background Log Compaction Implementation
 *
 * Algorithm walk-through:
 *
 *  1. shouldCompact()
 *     Fast check: is log file > threshold OR has interval elapsed?
 *
 *  2. runCompaction()
 *     a) Snapshot the current KeyDir (shared lock, O(n) copy).
 *     b) Open a temporary file "<log>.tmp".
 *     c) For each live key in the snapshot:
 *          - Read the record from the original log at keydir_entry.file_offset
 *          - Write it verbatim to the .tmp file
 *          - Track new offset in new_keydir map
 *     d) fdatasync the .tmp file.
 *     e) Call engine.swapLog(tmp_path, new_keydir)
 *          → acquires write lock, renames, re-opens, patches keydir
 *     f) Update compaction statistics.
 *
 * Notes on correctness:
 *  - Writes that happen between step (a) and step (e) land in the ORIGINAL
 *    log file. After swapLog(), those writes are lost — but wait:
 *    swapLog() replaces the keydir. If any key was updated after the snapshot,
 *    the engine's keydir already has a newer entry. The swap replaces keydir
 *    entirely with new_keydir, which only has entries as of the snapshot.
 *
 *  - To handle concurrent writes safely, after swapLog() the engine
 *    needs to replay any records written to the original log AFTER the
 *    snapshot was taken but BEFORE the rename. We solve this by:
 *      * Noting `snapshot_write_pos` = engine.write_pos at snapshot time.
 *      * After swap, replaying [snapshot_write_pos .. old_eof] from the
 *        original log (saved as "<log>.prev") into the new file.
 *
 *  Implementation here uses a simpler approach suitable for a portfolio
 *  engine: we hold the write lock during step (c)-(e) which stops new
 *  writes temporarily. Production systems (like Riak) use the multi-phase
 *  approach described above.
 */

#include "Compactor.hpp"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <cstring>
#include <stdexcept>
#include <system_error>
#include <iostream>
#include <chrono>

namespace flashdb {

// ── Constructor / Destructor ───────────────────────────────────────────────

Compactor::Compactor(FlashDB&             engine,
                     std::chrono::seconds interval,
                     uint64_t             size_threshold)
    : engine_(engine)
    , interval_(interval)
    , size_threshold_(size_threshold)
{
    stats_.last_run = std::chrono::steady_clock::now() - interval; // trigger soon
}

Compactor::~Compactor() {
    stop();
}

// ── Lifecycle ──────────────────────────────────────────────────────────────

void Compactor::start() {
    if (running_.exchange(true)) return;  // Already running
    worker_ = std::thread(&Compactor::backgroundLoop, this);
}

void Compactor::stop() {
    if (!running_.exchange(false)) return;
    cv_.notify_all();
    if (worker_.joinable()) worker_.join();
}

// ── Background Thread ──────────────────────────────────────────────────────

void Compactor::backgroundLoop() {
    while (running_) {
        std::unique_lock<std::mutex> lock(cv_mutex_);
        cv_.wait_for(lock, std::chrono::seconds(5), [this] {
            return !running_ || shouldCompact();
        });

        if (!running_) break;
        if (shouldCompact()) {
            lock.unlock();
            runCompaction();
        }
    }
}

bool Compactor::shouldCompact() const {
    auto now     = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                       now - stats_.last_run);

    if (elapsed >= interval_) return true;

    // Check file size threshold
    if (size_threshold_ > 0) {
        struct stat st{};
        if (::stat(engine_.log_path().c_str(), &st) == 0)
            if (static_cast<uint64_t>(st.st_size) >= size_threshold_)
                return true;
    }
    return false;
}

// ── Core Compaction Logic ──────────────────────────────────────────────────

bool Compactor::runCompaction() {
    // Only one compaction at a time
    std::unique_lock<std::mutex> compact_lock(compact_mutex_, std::try_to_lock);
    if (!compact_lock.owns_lock()) {
        std::cout << "[Compactor] Compaction already in progress, skipping.\n";
        return false;
    }

    std::cout << "[Compactor] Starting compaction...\n";
    auto t_start = std::chrono::steady_clock::now();

    // ── Lock the engine exclusively ───────────────────────────────────────
    std::unique_lock<std::shared_mutex> engine_lock(engine_.mutex_);

    // ── Step 1: Snapshot the current KeyDir ───────────────────────────────
    auto snapshot = engine_.keydir_;
    if (snapshot.empty()) {
        std::cout << "[Compactor] KeyDir is empty — nothing to compact.\n";
        stats_.last_run = std::chrono::steady_clock::now();
        return true;
    }

    // ── Step 2: Get file size BEFORE compaction (for stats) ───────────────
    uint64_t old_size = 0;
    {
        struct stat st{};
        if (::stat(engine_.log_path().c_str(), &st) == 0)
            old_size = static_cast<uint64_t>(st.st_size);
    }

    // ── Step 3: Open tmp file ─────────────────────────────────────────────
    auto tmp_path = engine_.log_path();
    tmp_path += ".tmp";
    int tmp_fd = ::open(tmp_path.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (tmp_fd < 0) {
        std::cerr << "[Compactor] Failed to open tmp file: "
                  << ::strerror(errno) << "\n";
        return false;
    }

    // ── Step 4: Open source log for reading ───────────────────────────────
    int src_fd = ::open(engine_.log_path().c_str(), O_RDONLY);
    if (src_fd < 0) {
        ::close(tmp_fd);
        std::cerr << "[Compactor] Failed to open source log.\n";
        return false;
    }

    // ── Step 5: Write only live records to tmp ────────────────────────────
    std::unordered_map<std::string, KeyDirEntry> new_keydir;
    new_keydir.reserve(snapshot.size());
    uint64_t  new_offset    = 0;
    uint64_t  records_kept  = 0;
    uint64_t  records_skipped = 0;

    // We'll read records from the source file using the snapshot offsets.
    // To avoid reading header twice, we re-read each live record in full.
    for (auto& [key, entry] : snapshot) {
        // Read the header at the recorded offset
        RecordHeader hdr{};
        ssize_t hn = ::pread(src_fd, &hdr, sizeof(hdr),
                             static_cast<off_t>(entry.file_offset));
        if (hn != static_cast<ssize_t>(sizeof(hdr))) {
            std::cerr << "[Compactor] Header read failed for key: " << key << "\n";
            ++records_skipped;
            continue;
        }

        // Sanity: key sizes should match
        if (hdr.key_sz != key.size() || hdr.val_sz == 0) {
            ++records_skipped;
            continue; // Tombstone or mismatch
        }

        // Verify this is still the live version (file_offset must match entry)
        // (We already snapshot the keydir so these are the current live entries)

        // Read value
        std::string value(hdr.val_sz, '\0');
        off_t val_off = static_cast<off_t>(entry.file_offset)
                      + static_cast<off_t>(sizeof(RecordHeader))
                      + static_cast<off_t>(hdr.key_sz);
        ssize_t vn = ::pread(src_fd, value.data(), hdr.val_sz, val_off);
        if (vn != static_cast<ssize_t>(hdr.val_sz)) {
            ++records_skipped;
            continue;
        }

        // Update header timestamp (preserve original) and recompute CRC
        // (timestamp already in hdr from original record — keep it)

        // Write to tmp: header + key + value via writev
        struct iovec iov[3];
        iov[0].iov_base = &hdr;
        iov[0].iov_len  = sizeof(hdr);
        iov[1].iov_base = const_cast<char*>(key.data());
        iov[1].iov_len  = key.size();
        iov[2].iov_base = value.data();
        iov[2].iov_len  = value.size();

        // Write at current end of tmp
        if (::lseek(tmp_fd, static_cast<off_t>(new_offset), SEEK_SET) < 0) {
            std::cerr << "[Compactor] lseek failed.\n";
            break;
        }
        ssize_t total = static_cast<ssize_t>(sizeof(hdr) + key.size() + value.size());
        ssize_t written = ::writev(tmp_fd, iov, 3);
        if (written != total) {
            std::cerr << "[Compactor] writev failed: " << ::strerror(errno) << "\n";
            break;
        }

        // Record new offset in new_keydir
        new_keydir[key] = KeyDirEntry{
            .file_offset = new_offset,
            .value_size  = hdr.val_sz,
            .timestamp   = hdr.timestamp
        };
        new_offset += static_cast<uint64_t>(total);
        ++records_kept;
    }

    ::close(src_fd);

    // ── Step 6: Sync the tmp file ─────────────────────────────────────────
    ::fsync(tmp_fd);
    ::close(tmp_fd);

    // ── Step 7: Get new file size ─────────────────────────────────────────
    uint64_t new_size = new_offset;
    uint64_t reclaimed = (old_size > new_size) ? (old_size - new_size) : 0;

    // ── Step 8: Atomic swap ───────────────────────────────────────────────
    engine_.swapLog(tmp_path, std::move(new_keydir));
    engine_.recordCompaction();

    // ── Step 9: Update stats ──────────────────────────────────────────────
    auto t_end = std::chrono::steady_clock::now();
    double elapsed_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();

    {
        std::lock_guard<std::mutex> sl(stats_mutex_);
        ++stats_.runs_completed;
        stats_.bytes_reclaimed = reclaimed;
        stats_.records_removed = records_skipped + (snapshot.size() - records_kept);
        stats_.last_run        = std::chrono::steady_clock::now();
    }

    std::cout << "[Compactor] Done in " << elapsed_ms << " ms. "
              << "Kept=" << records_kept
              << " Freed=" << reclaimed << " bytes"
              << " Old=" << old_size << " New=" << new_size << "\n";

    if (on_complete_) {
        CompactionStats s = stats();
        on_complete_(s);
    }

    return true;
}

// ── Public API ─────────────────────────────────────────────────────────────

bool Compactor::compact_now() {
    return runCompaction();
}

CompactionStats Compactor::stats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    return stats_;
}

void Compactor::onCompactionComplete(std::function<void(const CompactionStats&)> cb) {
    on_complete_ = std::move(cb);
}

} // namespace flashdb
