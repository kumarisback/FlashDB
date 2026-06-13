/**
 * stress_test.cpp  —  FlashDB Throughput Benchmark
 *
 * Runs three benchmark phases and reports ops/sec for each:
 *   1. Sequential WRITE:  Insert N keys sequentially
 *   2. Random READ:       Read N keys with random access pattern
 *   3. Mixed 80/20:       80% reads, 20% writes (realistic workload)
 *
 * Build & run:
 *   cmake -B build -DCMAKE_BUILD_TYPE=Release
 *   cmake --build build --target benchmark -j$(nproc)
 *   ./build/benchmark [num_ops]
 */

#include "FlashDB.hpp"
#include "Compactor.hpp"

#include <iostream>
#include <string>
#include <vector>
#include <random>
#include <chrono>
#include <iomanip>
#include <filesystem>
#include <functional>
#include <numeric>

using namespace flashdb;
using Clock = std::chrono::steady_clock;

// ── Timer ──────────────────────────────────────────────────────────────────

struct Timer {
    Clock::time_point start = Clock::now();
    double elapsedMs() const {
        return std::chrono::duration<double, std::milli>(Clock::now() - start).count();
    }
};

// ── Benchmark Runner ───────────────────────────────────────────────────────

struct BenchmarkResult {
    std::string name;
    uint64_t    ops;
    double      elapsed_ms;
    double      ops_per_sec() const { return ops / (elapsed_ms / 1000.0); }
    double      latency_us()  const { return (elapsed_ms * 1000.0) / ops; }
};

BenchmarkResult run(const std::string& name, uint64_t ops,
                    std::function<void(uint64_t i)> fn) {
    // Warmup
    for (uint64_t i = 0; i < std::min<uint64_t>(100, ops / 10); ++i)
        fn(i);

    Timer t;
    for (uint64_t i = 0; i < ops; ++i)
        fn(i);

    double elapsed = t.elapsedMs();
    return { name, ops, elapsed };
}

void printResult(const BenchmarkResult& r) {
    std::cout << std::left  << std::setw(28) << r.name
              << std::right << std::setw(10) << static_cast<uint64_t>(r.ops_per_sec())
              << " ops/s"
              << "   lat=" << std::fixed << std::setprecision(2) << r.latency_us() << " µs"
              << "   total=" << std::setprecision(1) << r.elapsed_ms << " ms\n";
}

void printHeader() {
    std::cout << "\n" << std::string(70, '=') << "\n";
    std::cout << "  FlashDB Benchmark\n";
    std::cout << std::string(70, '=') << "\n";
    std::cout << std::left  << std::setw(28) << "Benchmark"
              << std::right << std::setw(10) << "Throughput"
              << "         Latency        Time\n";
    std::cout << std::string(70, '-') << "\n";
}

// ── Main ───────────────────────────────────────────────────────────────────

int main(int argc, char** argv) {
    const uint64_t N = (argc > 1) ? std::stoull(argv[1]) : 100'000;
    const std::string BENCH_DIR = "./bench_data";

    std::cout << "Operations per benchmark: " << N << "\n";
    std::cout << "Data directory: " << BENCH_DIR << "\n";

    // Fresh database for each run
    if (std::filesystem::exists(BENCH_DIR))
        std::filesystem::remove_all(BENCH_DIR);

    FlashDB db(BENCH_DIR);

    // Pre-generate keys and values
    std::vector<std::string> keys(N);
    std::vector<std::string> values(N);
    for (uint64_t i = 0; i < N; ++i) {
        keys[i]   = "key:" + std::to_string(i);
        values[i] = "value_payload_" + std::to_string(i) + "_"
                  + std::string(64, 'x'); // ~80 byte values
    }

    // Random number generator for read pattern
    std::mt19937_64 rng(42);
    std::uniform_int_distribution<uint64_t> dist(0, N - 1);

    printHeader();

    // ── 1. Sequential Write ────────────────────────────────────────────────
    {
        auto r = run("Sequential PUT", N, [&](uint64_t i) {
            db.put(keys[i], values[i]);
        });
        printResult(r);
    }

    // ── 2. Sequential Read ─────────────────────────────────────────────────
    {
        auto r = run("Sequential GET", N, [&](uint64_t i) {
            auto v = db.get(keys[i]);
            (void)v;
        });
        printResult(r);
    }

    // ── 3. Random Read ─────────────────────────────────────────────────────
    {
        auto r = run("Random GET", N, [&](uint64_t) {
            auto v = db.get(keys[dist(rng)]);
            (void)v;
        });
        printResult(r);
    }

    // ── 4. Overwrite (update existing keys) ────────────────────────────────
    {
        auto r = run("Overwrite PUT", N, [&](uint64_t i) {
            db.put(keys[i % N], "updated_" + values[i % N]);
        });
        printResult(r);
    }

    // ── 5. Mixed 80/20 workload ────────────────────────────────────────────
    {
        auto r = run("Mixed 80R/20W", N, [&](uint64_t i) {
            if (i % 5 == 0) {
                db.put(keys[i % N], values[i % N]);
            } else {
                auto v = db.get(keys[dist(rng)]);
                (void)v;
            }
        });
        printResult(r);
    }

    // ── 6. DELETE ──────────────────────────────────────────────────────────
    {
        uint64_t del_n = N / 10; // Delete 10% of keys
        auto r = run("DEL (10% of keys)", del_n, [&](uint64_t i) {
            db.del(keys[i]);
        });
        printResult(r);
    }

    // ── Stats ──────────────────────────────────────────────────────────────
    std::cout << std::string(70, '=') << "\n";
    auto s = db.stats();
    std::cout << "\nFinal Engine Stats:\n"
              << "  Live keys       : " << s.total_keys       << "\n"
              << "  Log file size   : " << s.log_file_bytes / 1024 << " KB\n"
              << "  Total writes    : " << s.total_writes    << "\n"
              << "  Total reads     : " << s.total_reads     << "\n"
              << "  Total deletes   : " << s.total_deletes   << "\n";

    // ── Compaction Benchmark ───────────────────────────────────────────────
    std::cout << "\n[Benchmark] Triggering compaction...\n";
    Compactor compactor(db, std::chrono::seconds(9999), 0);
    Timer t;
    compactor.compact_now();
    double compact_ms = t.elapsedMs();

    auto s2 = db.stats();
    std::cout << "  Compaction time : " << std::fixed << std::setprecision(1)
              << compact_ms << " ms\n"
              << "  Log size after  : " << s2.log_file_bytes / 1024 << " KB\n"
              << "  Space saved     : "
              << (s.log_file_bytes > s2.log_file_bytes
                  ? (s.log_file_bytes - s2.log_file_bytes) / 1024
                  : 0)
              << " KB\n\n";

    // Cleanup
    std::filesystem::remove_all(BENCH_DIR);
    return 0;
}
