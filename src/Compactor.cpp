/**
 * Compactor.cpp  —  Asynchronous Non-Blocking Log Compaction
 */

#include "Compactor.hpp"
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <stdexcept>
#include <sys/stat.h>
#include <sys/uio.h>
#include <system_error>
#include <unistd.h>

namespace flashdb {

Compactor::Compactor(FlashDB &engine, std::chrono::seconds interval,
                     uint64_t size_threshold)
    : engine_(engine), interval_(interval), size_threshold_(size_threshold) {
  stats_.last_run = std::chrono::steady_clock::now() - interval;
}

Compactor::~Compactor() { stop(); }

void Compactor::start() {
  if (running_.exchange(true))
    return;
  worker_ = std::thread(&Compactor::backgroundLoop, this);
}

void Compactor::stop() {
  if (!running_.exchange(false))
    return;
  cv_.notify_all();
  if (worker_.joinable())
    worker_.join();
}

void Compactor::backgroundLoop() {
  while (running_) {
    std::unique_lock<std::mutex> lock(cv_mutex_);
    cv_.wait_for(lock, std::chrono::seconds(5),
                 [this] { return !running_ || shouldCompact(); });

    if (!running_)
      break;
    if (shouldCompact()) {
      lock.unlock();
      runCompaction();
    }
  }
}

bool Compactor::shouldCompact() const {
  auto now = std::chrono::steady_clock::now();
  auto elapsed =
      std::chrono::duration_cast<std::chrono::seconds>(now - stats_.last_run);
  if (elapsed >= interval_)
    return true;

  if (size_threshold_ > 0) {
    struct stat st{};
    if (::stat(engine_.log_path().c_str(), &st) == 0)
      if (static_cast<uint64_t>(st.st_size) >= size_threshold_)
        return true;
  }
  return false;
}

bool Compactor::runCompaction() {
  std::unique_lock<std::mutex> compact_lock(compact_mutex_, std::try_to_lock);
  if (!compact_lock.owns_lock()) {
    return false;
  }

  std::cout << "[Compactor] Starting non-blocking background compaction...\n";
  auto t_start = std::chrono::steady_clock::now();

  uint64_t snapshot_write_pos = 0;
  std::unordered_map<std::string, KeyDirEntry> snapshot;
  uint64_t old_size = 0;

  // ── Phase 1: Quick Snapshot (Shared Lock Only) ──────────────────────────
  // We only hold a shared lock to copy the index metadata. Writes remain fully
  // active!
  {
    std::shared_lock<std::shared_mutex> engine_lock(engine_.mutex_);
    snapshot = engine_.keydir_;
    snapshot_write_pos = engine_.write_pos_;

    struct stat st{};
    if (::stat(engine_.log_path().c_str(), &st) == 0)
      old_size = static_cast<uint64_t>(st.st_size);
  }

  if (snapshot.empty()) {
    stats_.last_run = std::chrono::steady_clock::now();
    return true;
  }

  // ── Phase 2: Copy Live Records to Temporary File ───────────────────────
  auto tmp_path = engine_.log_path();
  tmp_path += ".tmp";
  int tmp_fd = ::open(tmp_path.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
  if (tmp_fd < 0)
    return false;

  int src_fd = ::open(engine_.log_path().c_str(), O_RDONLY);
  if (src_fd < 0) {
    ::close(tmp_fd);
    return false;
  }

  std::unordered_map<std::string, KeyDirEntry> new_keydir;
  new_keydir.reserve(snapshot.size());
  uint64_t new_offset = 0;
  uint64_t records_kept = 0;
  uint64_t records_skipped = 0;

  for (auto &[key, entry] : snapshot) {
    // Fast skip if the record was originally written past our point of snapshot
    // evaluation
    if (entry.file_offset >= snapshot_write_pos)
      continue;

    RecordHeader hdr{};
    ssize_t hn = ::pread(src_fd, &hdr, sizeof(hdr),
                         static_cast<off_t>(entry.file_offset));
    if (hn != static_cast<ssize_t>(sizeof(hdr)) || hdr.key_sz != key.size() ||
        hdr.val_sz == 0) {
      ++records_skipped;
      continue;
    }

    std::string value(hdr.val_sz, '\0');
    off_t val_off = static_cast<off_t>(entry.file_offset) +
                    static_cast<off_t>(sizeof(RecordHeader)) +
                    static_cast<off_t>(hdr.key_sz);
    if (::pread(src_fd, value.data(), hdr.val_sz, val_off) !=
        static_cast<ssize_t>(hdr.val_sz)) {
      ++records_skipped;
      continue;
    }

    struct iovec iov[3];
    iov[0].iov_base = &hdr;
    iov[0].iov_len = sizeof(hdr);
    iov[1].iov_base = const_cast<char *>(key.data());
    iov[1].iov_len = key.size();
    iov[2].iov_base = value.data();
    iov[2].iov_len = value.size();

    // Removed redundant lseek call to eliminate OS system-call overhead
    ssize_t total =
        static_cast<ssize_t>(sizeof(hdr) + key.size() + value.size());
    if (::writev(tmp_fd, iov, 3) != total) {
      std::cerr << "[Compactor] writev failed.\n";
      break;
    }

    new_keydir[key] = KeyDirEntry{.file_offset = new_offset,
                                  .value_size = hdr.val_sz,
                                  .timestamp = hdr.timestamp};
    new_offset += static_cast<uint64_t>(total);
    ++records_kept;
  }

  // ── Phase 3: Delta Catch-Up and Atomic Exchange ───────────────────────
  // Acquire exclusive lock only at the very end to reconcile mutations that
  // occurred while we were writing out the temporary log file.
  {
    std::unique_lock<std::shared_mutex> engine_lock(engine_.mutex_);

    // For any key updated during Phase 2, preserve the newer active entry!
    for (auto &[key, current_entry] : engine_.keydir_) {
      // If the key has updates that happened AFTER our snapshot write position,
      // we read the absolute newest item and append it immediately to catch up.
      if (current_entry.file_offset >= snapshot_write_pos) {
        RecordHeader delta_hdr{};
        ::pread(src_fd, &delta_hdr, sizeof(delta_hdr),
                static_cast<off_t>(current_entry.file_offset));

        std::string delta_val(delta_hdr.val_sz, '\0');
        off_t d_v_off = static_cast<off_t>(current_entry.file_offset) +
                        sizeof(RecordHeader) + delta_hdr.key_sz;
        ::pread(src_fd, delta_val.data(), delta_hdr.val_sz, d_v_off);

        struct iovec iov[3];
        iov[0].iov_base = &delta_hdr;
        iov[0].iov_len = sizeof(delta_hdr);
        iov[1].iov_base = const_cast<char *>(key.data());
        iov[1].iov_len = key.size();
        iov[2].iov_base = delta_val.data();
        iov[2].iov_len = delta_val.size();

        ssize_t delta_total = static_cast<ssize_t>(
            sizeof(delta_hdr) + key.size() + delta_val.size());
        ::writev(tmp_fd, iov, 3);

        new_keydir[key] = KeyDirEntry{.file_offset = new_offset,
                                      .value_size = delta_hdr.val_sz,
                                      .timestamp = delta_hdr.timestamp};
        new_offset += static_cast<uint64_t>(delta_total);
      }
    }

    ::fdatasync(tmp_fd);
    ::close(tmp_fd);
    ::close(src_fd);

    // Execute atomic swap via POSIX filesystem primitives
    engine_.swapLog(tmp_path, std::move(new_keydir));
    engine_.write_pos_ = new_offset; // Update active engine offset bounds
    engine_.recordCompaction();
  }

  // ── Phase 4: Compute Metrics ───────────────────────────────────────────
  uint64_t reclaimed = (old_size > new_offset) ? (old_size - new_offset) : 0;
  auto t_end = std::chrono::steady_clock::now();
  double elapsed_ms =
      std::chrono::duration<double, std::milli>(t_end - t_start).count();

  {
    std::lock_guard<std::mutex> sl(stats_mutex_);
    ++stats_.runs_completed;
    stats_.bytes_reclaimed = reclaimed;
    stats_.records_removed = records_skipped + (snapshot.size() - records_kept);
    stats_.last_run = std::chrono::steady_clock::now();
  }

  std::cout << "[Compactor] Concurrent Compaction Finished in " << elapsed_ms
            << " ms. "
            << "Freed=" << reclaimed << " bytes\n";

  if (on_complete_) {
    on_complete_(stats());
  }

  return true;
}

bool Compactor::compact_now() { return runCompaction(); }

CompactionStats Compactor::stats() const {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  return stats_;
}

void Compactor::onCompactionComplete(
    std::function<void(const CompactionStats &)> cb) {
  on_complete_ = std::move(cb);
}

} // namespace flashdb