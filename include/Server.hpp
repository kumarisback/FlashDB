/**
 * Server.hpp  —  Async TCP Network Server for FlashDB
 *
 * Protocol: RESP-like text protocol (Redis-compatible subset)
 * ----------------------------------------------------------
 * Commands (newline-terminated, space-delimited):
 *
 *   SET <key> <value>   →  +OK\r\n
 *   GET <key>           →  $<len>\r\n<value>\r\n  |  $-1\r\n (nil)
 *   DEL <key>           →  :1\r\n  |  :0\r\n
 *   EXISTS <key>        →  :1\r\n  |  :0\r\n
 *   KEYS                →  *<count>\r\n$<len>\r\n<key>\r\n ...
 *   PING                →  +PONG\r\n
 *   STATS               →  Inline stats block
 *   COMPACT             →  +OK\r\n  (triggers immediate compaction)
 *   QUIT                →  +BYE\r\n  (closes connection)
 *
 * I/O Model:
 * ----------
 *  - Non-blocking sockets (O_NONBLOCK)
 *  - Cross-platform event loop:
 *      Linux → epoll (edge-triggered EPOLLET)
 *      macOS → kqueue
 *  - Single event-dispatch thread accepts connections and reads data.
 *  - Command execution dispatched to a fixed thread pool so the event
 *    loop never blocks on engine I/O.
 *  - Per-connection read/write buffers; backpressure via non-blocking send.
 *
 * Capacity:
 *  - Up to 10,000 concurrent connections (limited by ulimit -n)
 *  - Worker pool sized to std::thread::hardware_concurrency()
 */

#pragma once

#include "FlashDB.hpp"
#include "Compactor.hpp"

#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include <functional>
#include <memory>
#include <unordered_map>
#include <deque>

namespace flashdb {

// ── Per-Connection State ───────────────────────────────────────────────────
struct Connection {
    int          fd{-1};
    std::string  read_buf;     // Accumulates incoming bytes
    std::string  write_buf;    // Pending outbound bytes
    bool         closing{false};

    explicit Connection(int fd) : fd(fd) {}
};

// ── Server Configuration ───────────────────────────────────────────────────
struct ServerConfig {
    std::string host         = "0.0.0.0";
    int         port         = 6399;          // Default FlashDB port
    int         backlog      = 1024;
    size_t      worker_threads = 0;           // 0 = hardware_concurrency()
    size_t      max_connections = 10000;
    size_t      read_buf_size   = 4096;       // Per-connection read chunk
};

// ── Server Statistics ──────────────────────────────────────────────────────
struct ServerStats {
    std::atomic<uint64_t> connections_total{0};
    std::atomic<uint64_t> connections_active{0};
    std::atomic<uint64_t> commands_processed{0};
    std::atomic<uint64_t> bytes_read{0};
    std::atomic<uint64_t> bytes_written{0};
};

// ── Async TCP Server ───────────────────────────────────────────────────────
class Server {
public:
    /**
     * Create a server bound to the given engine (and optionally a Compactor).
     * Does not start listening until start() is called.
     */
    explicit Server(FlashDB&       engine,
                    Compactor*     compactor = nullptr,
                    ServerConfig   cfg       = {});

    ~Server();

    // Non-copyable
    Server(const Server&)            = delete;
    Server& operator=(const Server&) = delete;

    /**
     * Bind the socket, start the worker pool, and enter the event loop.
     * Blocks until stop() is called from another thread or a signal handler.
     * @throws std::runtime_error on bind/listen failure.
     */
    void start();

    /**
     * Signal the event loop to stop. Waits for graceful shutdown:
     *   - Closes the listen socket (stops new accepts)
     *   - Drains in-flight commands
     *   - Joins worker threads
     * Thread-safe; can be called from a signal handler.
     */
    void stop();

    /**
     * Return a reference to the live server statistics.
     */
    const ServerStats& stats() const { return stats_; }

private:
    // ── Socket Setup ───────────────────────────────────────────────────────
    void bindAndListen();
    void setNonBlocking(int fd);

    // ── Event Loop ─────────────────────────────────────────────────────────
    void eventLoop();             // Main I/O dispatch thread
    void workerLoop();            // Thread pool workers

    void acceptNewConnection(int listen_fd);
    void handleRead(int fd);
    void handleWrite(int fd);
    void closeConnection(int fd);

    // ── Protocol ───────────────────────────────────────────────────────────
    /** Parse as many complete commands as possible from conn.read_buf.
     *  Appends responses directly to conn.write_buf. */
    void processCommands(Connection& conn);
    std::string executeCommand(const std::vector<std::string>& tokens);

    std::string cmdSet(const std::vector<std::string>& t);
    std::string cmdGet(const std::vector<std::string>& t);
    std::string cmdDel(const std::vector<std::string>& t);
    std::string cmdExists(const std::vector<std::string>& t);
    std::string cmdKeys(const std::vector<std::string>& t);
    std::string cmdStats(const std::vector<std::string>& t);
    std::string cmdCompact(const std::vector<std::string>& t);

    // RESP helpers
    static std::string respOK();
    static std::string respErr(std::string_view msg);
    static std::string respBulk(std::string_view s);
    static std::string respNil();
    static std::string respInt(int64_t n);
    static std::string respArray(const std::vector<std::string>& items);

    // ── Platform I/O multiplexer ───────────────────────────────────────────
    int  createEventFd();          // epoll_create1 or kqueue
    void registerFd(int efd, int fd, bool write = false);
    void modifyFd(int efd, int fd, bool write);
    void unregisterFd(int efd, int fd);

    // ── State ──────────────────────────────────────────────────────────────
    FlashDB&      engine_;
    Compactor*    compactor_;
    ServerConfig  cfg_;

    int           listen_fd_{-1};
    int           event_fd_{-1};   // epoll or kqueue fd

    std::atomic<bool>  running_{false};
    std::thread        io_thread_;
    std::vector<std::thread> workers_;

    // Connection map — protected by conn_mutex_
    std::mutex                              conn_mutex_;
    std::unordered_map<int, std::unique_ptr<Connection>> conns_;

    // Work queue — commands to execute on worker threads
    std::mutex              work_mutex_;
    std::condition_variable work_cv_;
    struct WorkItem {
        int                      fd;
        std::vector<std::string> tokens;
    };
    std::deque<WorkItem>    work_queue_;

    ServerStats stats_;
};

} // namespace flashdb
