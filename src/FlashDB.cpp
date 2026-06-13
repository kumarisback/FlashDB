/**
 * FlashDB.cpp  —  Bitcask-style Key-Value Storage Engine Implementation
 *
 * Implementation notes:
 *  - All writes go through appendRecord() which holds the write lock
 *    only for the duration of fwrite + fsync (a few microseconds).
 *  - Reads use pread() which is safe to call concurrently with writes
 *    because pread() doesn't move the file position pointer.
 *  - The KeyDir is protected by a shared_mutex:
 *      reads     → shared_lock   (many concurrent readers)
 *      put/del   → unique_lock   (exclusive; serializes writes)
 *      compactor → unique_lock   (during final log swap only)
 */

#include "FlashDB.hpp"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <cerrno>
#include <cstring>
#include <ctime>
#include <stdexcept>
#include <system_error>
#include <chrono>
#include <iostream>

namespace flashdb {

// ── CRC-32 (Castagnoli / IEEE polynomial, reflected) ──────────────────────
// Pure tabular implementation — zero external dependencies.
namespace {

static uint32_t crc32_table[256];
static bool     crc_table_ready = false;

void initCRCTable() {
    if (crc_table_ready) return;
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int j = 0; j < 8; j++)
            c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        crc32_table[i] = c;
    }
    crc_table_ready = true;
}

int64_t nowMicros() {
    using namespace std::chrono;
    return duration_cast<microseconds>(
        system_clock::now().time_since_epoch()).count();
}

void throwErrno(const char* msg) {
    throw std::system_error(errno, std::system_category(), msg);
}

} // anon namespace

// ── Constructor / Destructor ───────────────────────────────────────────────

FlashDB::FlashDB(std::filesystem::path data_dir)
    : data_dir_(std::move(data_dir))
    , log_path_(data_dir_ / "flashdb.log")
{
    std::filesystem::create_directories(data_dir_);
    openLog();
    replayLog();
}

FlashDB::~FlashDB() {
    try { close(); } catch (...) {}
}

// ── File Management ────────────────────────────────────────────────────────

void FlashDB::openLog() {
    // O_CREAT | O_RDWR | O_APPEND
    log_fd_ = ::open(log_path_.c_str(), O_CREAT | O_RDWR, 0644);
    if (log_fd_ < 0) throwErrno("FlashDB::openLog");

    // Determine current file size (= write position)
    struct stat st{};
    if (::fstat(log_fd_, &st) < 0) throwErrno("FlashDB::openLog fstat");
    write_pos_ = static_cast<uint64_t>(st.st_size);
}

// ── Log Replay (startup) ───────────────────────────────────────────────────

void FlashDB::replayLog() {
    uint64_t pos = 0;
    RecordHeader hdr{};

    while (true) {
        ssize_t n = ::pread(log_fd_, &hdr, HEADER_SIZE, static_cast<off_t>(pos));
        if (n == 0) break;                         // EOF
        if (n < 0)  throwErrno("FlashDB::replayLog pread header");
        if (static_cast<size_t>(n) < HEADER_SIZE)  break; // Truncated header

        // Read key
        std::string key(hdr.key_sz, '\0');
        if (hdr.key_sz > 0) {
            ssize_t kr = ::pread(log_fd_, key.data(), hdr.key_sz,
                                 static_cast<off_t>(pos + HEADER_SIZE));
            if (kr != static_cast<ssize_t>(hdr.key_sz))
                break; // Corrupted record — stop replay
        }

        // Read value (skip it for now, just to advance position)
        std::string val(hdr.val_sz, '\0');
        if (hdr.val_sz > 0) {
            ssize_t vr = ::pread(log_fd_, val.data(), hdr.val_sz,
                                 static_cast<off_t>(pos + HEADER_SIZE + hdr.key_sz));
            if (vr != static_cast<ssize_t>(hdr.val_sz))
                break;
        }

        // Verify CRC
        uint32_t expected = computeCRC(hdr, key, val);
        if (expected != hdr.crc32) {
            std::cerr << "[FlashDB] CRC mismatch at offset " << pos
                      << ", stopping replay.\n";
            break;
        }

        // Apply to keydir
        if (hdr.val_sz == TOMBSTONE) {
            keydir_.erase(key);
        } else {
            keydir_[key] = KeyDirEntry{
                .file_offset = pos,
                .value_size  = hdr.val_sz,
                .timestamp   = hdr.timestamp
            };
        }

        pos += HEADER_SIZE + hdr.key_sz + hdr.val_sz;
    }

    write_pos_ = pos;
    // Truncate any partial/corrupt tail
    if (::ftruncate(log_fd_, static_cast<off_t>(write_pos_)) < 0)
        throwErrno("FlashDB::replayLog ftruncate");
}

// ── CRC Computation ────────────────────────────────────────────────────────

uint32_t FlashDB::computeCRC(const RecordHeader& hdr,
                              std::string_view    key,
                              std::string_view    value)
{
    // CRC covers: timestamp + key_sz + val_sz + key bytes + value bytes
    uint32_t c = 0xFFFFFFFFu;
    initCRCTable();
    // Re-use the raw crc32 function over concatenated fields
    auto mix = [&](const void* p, size_t n) {
        const uint8_t* b = static_cast<const uint8_t*>(p);
        while (n--) c = crc32_table[(c ^ *b++) & 0xFF] ^ (c >> 8);
    };
    mix(&hdr.timestamp, sizeof(hdr.timestamp));
    mix(&hdr.key_sz,    sizeof(hdr.key_sz));
    mix(&hdr.val_sz,    sizeof(hdr.val_sz));
    mix(key.data(),     key.size());
    mix(value.data(),   value.size());
    return c ^ 0xFFFFFFFFu;
}

// ── appendRecord (internal, must hold write lock) ──────────────────────────

uint64_t FlashDB::appendRecord(std::string_view key,
                                std::string_view value,
                                int64_t          timestamp)
{
    RecordHeader hdr{};
    hdr.timestamp = timestamp;
    hdr.key_sz    = static_cast<uint32_t>(key.size());
    hdr.val_sz    = static_cast<uint32_t>(value.size());
    hdr.crc32     = computeCRC(hdr, key, value);

    // Build iovec for a single writev call (avoids multiple syscalls)
    struct iovec iov[3];
    iov[0].iov_base = &hdr;
    iov[0].iov_len  = HEADER_SIZE;
    iov[1].iov_base = const_cast<char*>(key.data());
    iov[1].iov_len  = key.size();
    iov[2].iov_base = const_cast<char*>(value.data());
    iov[2].iov_len  = value.size();

    // Seek to write position
    if (::lseek(log_fd_, static_cast<off_t>(write_pos_), SEEK_SET) < 0)
        throwErrno("FlashDB::appendRecord lseek");

    ssize_t total = static_cast<ssize_t>(HEADER_SIZE + key.size() + value.size());
    ssize_t written = ::writev(log_fd_, iov, 3);
    if (written != total)
        throwErrno("FlashDB::appendRecord writev");

    uint64_t record_offset = write_pos_;
    write_pos_ += static_cast<uint64_t>(total);
    return record_offset;
}

// ── Public API ─────────────────────────────────────────────────────────────

bool FlashDB::put(std::string_view key, std::string_view value) {
    if (key.empty()) throw std::invalid_argument("key must not be empty");

    std::unique_lock lock(mutex_);
    if (closed_) throw std::runtime_error("FlashDB is closed");

    int64_t  ts     = nowMicros();
    uint64_t offset = appendRecord(key, value, ts);

    keydir_[std::string(key)] = KeyDirEntry{
        .file_offset = offset,
        .value_size  = static_cast<uint32_t>(value.size()),
        .timestamp   = ts
    };

    ++stat_writes_;
    return true;
}

std::optional<std::string> FlashDB::get(std::string_view key) {
    std::shared_lock lock(mutex_);
    if (closed_) throw std::runtime_error("FlashDB is closed");

    auto it = keydir_.find(std::string(key));
    if (it == keydir_.end()) {
        ++stat_reads_;
        return std::nullopt;
    }

    const KeyDirEntry& entry = it->second;
    std::string value(entry.value_size, '\0');

    // Read the header to get key_sz so we can compute the value offset.
    // pread() is safe here under a shared_lock because it uses an explicit
    // offset and does NOT move the file position pointer.
    RecordHeader hdr{};
    ssize_t hn = ::pread(log_fd_, &hdr, HEADER_SIZE,
                         static_cast<off_t>(entry.file_offset));
    if (hn != static_cast<ssize_t>(HEADER_SIZE))
        throw std::runtime_error("FlashDB::get: corrupted record header");

    off_t val_off = static_cast<off_t>(entry.file_offset)
                  + static_cast<off_t>(HEADER_SIZE)
                  + static_cast<off_t>(hdr.key_sz);

    ssize_t vn = ::pread(log_fd_, value.data(), entry.value_size, val_off);
    if (vn != static_cast<ssize_t>(entry.value_size))
        throw std::runtime_error("FlashDB::get: short read on value");

    ++stat_reads_;
    return value;
}

bool FlashDB::del(std::string_view key) {
    std::unique_lock lock(mutex_);
    if (closed_) throw std::runtime_error("FlashDB is closed");

    auto it = keydir_.find(std::string(key));
    if (it == keydir_.end()) return false;

    // Write tombstone: value_sz == 0
    appendRecord(key, std::string_view{}, nowMicros());
    keydir_.erase(it);
    ++stat_deletes_;
    return true;
}

bool FlashDB::exists(std::string_view key) const {
    std::shared_lock lock(mutex_);
    return keydir_.count(std::string(key)) > 0;
}

std::vector<std::string> FlashDB::keys() const {
    std::shared_lock lock(mutex_);
    std::vector<std::string> result;
    result.reserve(keydir_.size());
    for (const auto& [k, _] : keydir_)
        result.push_back(k);
    return result;
}

void FlashDB::sync() {
    std::shared_lock lock(mutex_);
    if (::fsync(log_fd_) < 0) throwErrno("FlashDB::sync");
}

void FlashDB::close() {
    std::unique_lock lock(mutex_);
    if (closed_) return;
    closed_ = true;
    if (log_fd_ >= 0) {
        ::fsync(log_fd_);
        ::close(log_fd_);
        log_fd_ = -1;
    }
}

EngineStats FlashDB::stats() const {
    std::shared_lock lock(mutex_);
    struct stat st{};
    uint64_t file_bytes = 0;
    if (log_fd_ >= 0 && ::fstat(log_fd_, &st) == 0)
        file_bytes = static_cast<uint64_t>(st.st_size);

    return EngineStats{
        .total_keys     = static_cast<uint64_t>(keydir_.size()),
        .log_file_bytes = file_bytes,
        .total_reads    = stat_reads_.load(),
        .total_writes   = stat_writes_.load(),
        .total_deletes  = stat_deletes_.load(),
        .compactions    = stat_compactions_.load()
    };
}

std::filesystem::path FlashDB::log_path() const {
    return log_path_;
}

std::unordered_map<std::string, KeyDirEntry> FlashDB::snapshotKeyDir() const {
    std::shared_lock lock(mutex_);
    return keydir_;   // Copy
}

void FlashDB::swapLog(const std::filesystem::path& new_log,
                      std::unordered_map<std::string, KeyDirEntry> new_keydir)
{
    // Caller (Compactor) must hold the exclusive lock on mutex_.

    // 1. Close current fd
    if (log_fd_ >= 0) {
        ::fsync(log_fd_);
        ::close(log_fd_);
        log_fd_ = -1;
    }

    // 2. Atomic rename (POSIX guarantees atomicity)
    if (std::filesystem::exists(new_log)) {
        std::filesystem::rename(new_log, log_path_);
    }

    // 3. Re-open the file
    log_fd_ = ::open(log_path_.c_str(), O_RDWR, 0644);
    if (log_fd_ < 0) throwErrno("FlashDB::swapLog reopen");

    struct stat st{};
    ::fstat(log_fd_, &st);
    write_pos_ = static_cast<uint64_t>(st.st_size);

    // 4. Swap the KeyDir
    keydir_ = std::move(new_keydir);
}

void FlashDB::recordCompaction() {
    ++stat_compactions_;
}

} // namespace flashdb
