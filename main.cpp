/**
 * main.cpp  —  FlashDB Server Entry Point
 *
 * Usage:
 *   ./FlashDB [--port <port>] [--data <dir>] [--compact-interval <seconds>]
 *             [--compact-threshold <bytes>]
 *
 * Defaults:
 *   port              = 6399
 *   data              = ./data
 *   compact-interval  = 60 seconds
 *   compact-threshold = 67108864 (64 MB)
 */

#include "FlashDB.hpp"
#include "Compactor.hpp"
#include "Server.hpp"

#include <csignal>
#include <iostream>
#include <string>
#include <map>
#include <atomic>
#include <stdexcept>
#include <filesystem>
#include <chrono>

// ── Global server pointer for signal handler ───────────────────────────────
static flashdb::Server* g_server = nullptr;

void signalHandler(int sig) {
    std::cout << "\n[FlashDB] Received signal " << sig << ", shutting down...\n";
    if (g_server) g_server->stop();
}

// ── CLI parsing ────────────────────────────────────────────────────────────

struct Config {
    int         port              = 6399;
    std::string data_dir          = "./data";
    int         compact_interval  = 60;
    uint64_t    compact_threshold = 64ULL * 1024 * 1024;
    size_t      workers           = 0;
};

Config parseArgs(int argc, char** argv) {
    Config cfg;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto nextArg = [&]() -> std::string {
            if (i + 1 >= argc)
                throw std::invalid_argument("Missing value for " + arg);
            return argv[++i];
        };
        if      (arg == "--port")              cfg.port              = std::stoi(nextArg());
        else if (arg == "--data")              cfg.data_dir          = nextArg();
        else if (arg == "--compact-interval")  cfg.compact_interval  = std::stoi(nextArg());
        else if (arg == "--compact-threshold") cfg.compact_threshold = std::stoull(nextArg());
        else if (arg == "--workers")           cfg.workers           = std::stoul(nextArg());
        else if (arg == "--help") {
            std::cout <<
                "Usage: FlashDB [options]\n"
                "  --port <n>               Listen port (default: 6399)\n"
                "  --data <dir>             Data directory (default: ./data)\n"
                "  --compact-interval <s>   GC interval in seconds (default: 60)\n"
                "  --compact-threshold <b>  GC triggers when log > bytes (default: 67108864)\n"
                "  --workers <n>            Thread pool size (default: hw_concurrency)\n";
            std::exit(0);
        }
    }
    return cfg;
}

// ── Banner ─────────────────────────────────────────────────────────────────

void printBanner(const Config& cfg) {
    std::cout << R"(
  _____ _           _     ____  ____
 |  ___| | __ _ ___| |__ |  _ \| __ )
 | |_  | |/ _` / __| '_ \| | | |  _ \
 |  _| | | (_| \__ \ | | | |_| | |_) |
 |_|   |_|\__,_|___/_| |_|____/|____/

  Bitcask-style Key-Value Engine
  Port    : )" << cfg.port << R"(
  Data    : )" << cfg.data_dir << R"(
  Compact : every )" << cfg.compact_interval << R"(s | threshold )"
         << (cfg.compact_threshold / 1024 / 1024) << R"( MB
  Protocol: RESP (redis-cli compatible)
)" << "\n";
}

// ── main ───────────────────────────────────────────────────────────────────

int main(int argc, char** argv) {
    Config cfg;
    try {
        cfg = parseArgs(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << "Argument error: " << e.what() << "\n";
        return 1;
    }

    printBanner(cfg);

    // ── 1. Open storage engine ─────────────────────────────────────────────
    flashdb::FlashDB engine(cfg.data_dir);
    std::cout << "[FlashDB] Engine opened at " << cfg.data_dir << "\n";
    {
        auto s = engine.stats();
        std::cout << "[FlashDB] Recovered " << s.total_keys
                  << " keys from " << s.log_file_bytes << " bytes on disk\n";
    }

    // ── 2. Start compactor ─────────────────────────────────────────────────
    flashdb::Compactor compactor(
        engine,
        std::chrono::seconds(cfg.compact_interval),
        cfg.compact_threshold
    );

    compactor.onCompactionComplete([](const flashdb::CompactionStats& s) {
        std::cout << "[Compactor] Cycle #" << s.runs_completed
                  << " freed " << s.bytes_reclaimed << " bytes\n";
    });

    compactor.start();
    std::cout << "[Compactor] Background GC started\n";

    // ── 3. Start TCP server ────────────────────────────────────────────────
    flashdb::ServerConfig srv_cfg;
    srv_cfg.port           = cfg.port;
    srv_cfg.worker_threads = cfg.workers;

    flashdb::Server server(engine, &compactor, std::move(srv_cfg));
    g_server = &server;

    // Register signal handlers for graceful shutdown
    std::signal(SIGINT,  signalHandler);
    std::signal(SIGTERM, signalHandler);
    std::signal(SIGPIPE, SIG_IGN);   // Ignore broken pipe on send()

    std::cout << "[FlashDB] Ready. Connect with: redis-cli -p "
              << cfg.port << "\n\n";

    // Blocking call — runs until stop() is called
    server.start();

    // ── 4. Graceful shutdown ───────────────────────────────────────────────
    compactor.stop();
    engine.sync();
    engine.close();

    std::cout << "[FlashDB] Goodbye.\n";
    return 0;
}
