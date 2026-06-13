/**
 * Server.cpp  —  Async TCP Server Implementation
 *
 * Platform support:
 *   macOS  → kqueue (EVFILT_READ / EVFILT_WRITE)
 *   Linux  → epoll  (EPOLLIN / EPOLLOUT | EPOLLET)
 *
 * Thread model:
 *   1 x IO thread   — runs the event loop (accept, read, write)
 *   N x Workers     — parse protocol and call engine (N = hw_concurrency)
 *
 * The IO thread dispatches parsed commands to the work queue as WorkItems.
 * Workers pull WorkItems, call executeCommand(), and push the response
 * back into the connection's write_buf — then arm EPOLLOUT/EVFILT_WRITE
 * on the connection fd so the IO thread flushes it.
 */

#include "Server.hpp"

#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <sstream>
#include <iostream>
#include <algorithm>
#include <cassert>
#include <stdexcept>
#include <system_error>
#include <chrono>
#include <iomanip>

#ifdef __APPLE__
  #include <sys/event.h>
  #define USE_KQUEUE 1
#else
  #include <sys/epoll.h>
  #define USE_EPOLL 1
#endif

namespace flashdb {

// ── Helpers ────────────────────────────────────────────────────────────────

namespace {

void throwErrno(const char* ctx) {
    throw std::system_error(errno, std::system_category(), ctx);
}

std::vector<std::string> tokenize(const std::string& line) {
    std::vector<std::string> tokens;
    std::istringstream ss(line);
    std::string tok;
    while (ss >> tok) tokens.push_back(tok);
    return tokens;
}

std::string toUpper(std::string s) {
    for (auto& c : s) c = static_cast<char>(::toupper(static_cast<unsigned char>(c)));
    return s;
}

} // anon namespace

// ── Constructor / Destructor ───────────────────────────────────────────────

Server::Server(FlashDB& engine, Compactor* compactor, ServerConfig cfg)
    : engine_(engine)
    , compactor_(compactor)
    , cfg_(std::move(cfg))
{
    if (cfg_.worker_threads == 0)
        cfg_.worker_threads = std::max(1u, std::thread::hardware_concurrency());
}

Server::~Server() {
    stop();
}

// ── Socket setup ───────────────────────────────────────────────────────────

void Server::setNonBlocking(int fd) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) throwErrno("Server::setNonBlocking F_GETFL");
    if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
        throwErrno("Server::setNonBlocking F_SETFL");
}

void Server::bindAndListen() {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) throwErrno("Server: socket()");

    int yes = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes));
    ::setsockopt(listen_fd_, IPPROTO_TCP, TCP_NODELAY,  &yes, sizeof(yes));

    setNonBlocking(listen_fd_);

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(static_cast<uint16_t>(cfg_.port));
    ::inet_pton(AF_INET, cfg_.host.c_str(), &addr.sin_addr);

    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
        throwErrno("Server: bind()");

    if (::listen(listen_fd_, cfg_.backlog) < 0)
        throwErrno("Server: listen()");

    std::cout << "[Server] Listening on " << cfg_.host
              << ":" << cfg_.port << "\n";
}

// ── Platform event fd ──────────────────────────────────────────────────────

int Server::createEventFd() {
#ifdef USE_KQUEUE
    int kq = ::kqueue();
    if (kq < 0) throwErrno("Server: kqueue()");
    return kq;
#else
    int ep = ::epoll_create1(EPOLL_CLOEXEC);
    if (ep < 0) throwErrno("Server: epoll_create1()");
    return ep;
#endif
}

void Server::registerFd(int efd, int fd, bool want_write) {
#ifdef USE_KQUEUE
    struct kevent changes[2];
    int n = 0;
    EV_SET(&changes[n++], fd, EVFILT_READ,  EV_ADD | EV_ENABLE | EV_CLEAR, 0, 0, nullptr);
    if (want_write)
        EV_SET(&changes[n++], fd, EVFILT_WRITE, EV_ADD | EV_ENABLE | EV_CLEAR, 0, 0, nullptr);
    ::kevent(efd, changes, n, nullptr, 0, nullptr);
#else
    struct epoll_event ev{};
    ev.data.fd = fd;
    ev.events  = EPOLLIN | EPOLLET;
    if (want_write) ev.events |= EPOLLOUT;
    ::epoll_ctl(efd, EPOLL_CTL_ADD, fd, &ev);
#endif
}

void Server::modifyFd(int efd, int fd, bool want_write) {
#ifdef USE_KQUEUE
    struct kevent changes[2];
    int n = 0;
    EV_SET(&changes[n++], fd, EVFILT_READ,  EV_ADD | EV_ENABLE | EV_CLEAR, 0, 0, nullptr);
    if (want_write)
        EV_SET(&changes[n++], fd, EVFILT_WRITE, EV_ADD | EV_ENABLE | EV_CLEAR, 0, 0, nullptr);
    else
        EV_SET(&changes[n++], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    ::kevent(efd, changes, n, nullptr, 0, nullptr);
#else
    struct epoll_event ev{};
    ev.data.fd = fd;
    ev.events  = EPOLLIN | EPOLLET;
    if (want_write) ev.events |= EPOLLOUT;
    ::epoll_ctl(efd, EPOLL_CTL_MOD, fd, &ev);
#endif
}

void Server::unregisterFd(int efd, int fd) {
#ifdef USE_KQUEUE
    struct kevent changes[2];
    EV_SET(&changes[0], fd, EVFILT_READ,  EV_DELETE, 0, 0, nullptr);
    EV_SET(&changes[1], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    ::kevent(efd, changes, 2, nullptr, 0, nullptr);
#else
    ::epoll_ctl(efd, EPOLL_CTL_DEL, fd, nullptr);
#endif
}

// ── Connection Management ──────────────────────────────────────────────────

void Server::acceptNewConnection(int listen_fd) {
    while (true) {
        struct sockaddr_in peer{};
        socklen_t peer_len = sizeof(peer);
        int cfd = ::accept(listen_fd,
                           reinterpret_cast<sockaddr*>(&peer),
                           &peer_len);
        if (cfd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            if (errno == EINTR) continue;
            std::cerr << "[Server] accept error: " << ::strerror(errno) << "\n";
            break;
        }

        // Make non-blocking on all platforms
        setNonBlocking(cfd);

        int yes = 1;
        ::setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

        {
            std::lock_guard<std::mutex> lock(conn_mutex_);
            if (conns_.size() >= cfg_.max_connections) {
                const char* msg = "-ERR max connections reached\r\n";
                ::send(cfd, msg, ::strlen(msg), 0);
                ::close(cfd);
                continue;
            }
            conns_.emplace(cfd, std::make_unique<Connection>(cfd));
        }

        registerFd(event_fd_, cfd, false);

        ++stats_.connections_total;
        ++stats_.connections_active;

        char addr_str[INET_ADDRSTRLEN];
        ::inet_ntop(AF_INET, &peer.sin_addr, addr_str, sizeof(addr_str));
        std::cout << "[Server] New connection fd=" << cfd
                  << " from " << addr_str << ":" << ntohs(peer.sin_port) << "\n";
    }
}

void Server::closeConnection(int fd) {
    std::lock_guard<std::mutex> lock(conn_mutex_);
    auto it = conns_.find(fd);
    if (it == conns_.end()) return;

    unregisterFd(event_fd_, fd);
    ::close(fd);
    conns_.erase(it);
    --stats_.connections_active;
}

// ── Read/Write ─────────────────────────────────────────────────────────────

void Server::handleRead(int fd) {
    std::unique_lock<std::mutex> lock(conn_mutex_);
    auto it = conns_.find(fd);
    if (it == conns_.end()) return;
    Connection* conn = it->second.get();
    lock.unlock();

    char buf[4096];
    while (true) {
        ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        if (n > 0) {
            conn->read_buf.append(buf, static_cast<size_t>(n));
            stats_.bytes_read += static_cast<uint64_t>(n);
        } else if (n == 0) {
            closeConnection(fd);
            return;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            if (errno == EINTR) continue;
            closeConnection(fd);
            return;
        }
    }

    // Parse and dispatch commands
    processCommands(*conn);

    lock.lock();
    it = conns_.find(fd);
    if (it != conns_.end() && conn->closing && conn->write_buf.empty()) {
        lock.unlock();
        closeConnection(fd);
        return;
    }
}

void Server::handleWrite(int fd) {
    std::unique_lock<std::mutex> lock(conn_mutex_);
    auto it = conns_.find(fd);
    if (it == conns_.end()) return;
    Connection* conn = it->second.get();

    while (!conn->write_buf.empty()) {
        ssize_t n = ::send(fd, conn->write_buf.data(), conn->write_buf.size(), MSG_NOSIGNAL);
        if (n > 0) {
            conn->write_buf.erase(0, static_cast<size_t>(n));
            stats_.bytes_written += static_cast<uint64_t>(n);
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            if (errno == EINTR) continue;
            lock.unlock();
            closeConnection(fd);
            return;
        }
    }

    if (conn->write_buf.empty()) {
        if (conn->closing) {
            lock.unlock();
            closeConnection(fd);
            return;
        }
        modifyFd(event_fd_, fd, false); // Stop watching for write-ready
    }
}

// ── Protocol Parsing ───────────────────────────────────────────────────────

void Server::processCommands(Connection& conn) {
    // Simple line-based protocol: commands end with \n (or \r\n)
    std::string& buf = conn.read_buf;
    while (true) {
        auto pos = buf.find('\n');
        if (pos == std::string::npos) break;

        std::string line = buf.substr(0, pos);
        buf.erase(0, pos + 1);

        // Strip \r
        if (!line.empty() && line.back() == '\r')
            line.pop_back();

        if (line.empty()) continue;

        auto tokens = tokenize(line);
        if (tokens.empty()) continue;

        bool is_quit = (toUpper(tokens[0]) == "QUIT");

        // Push to work queue
        {
            std::lock_guard<std::mutex> lock(work_mutex_);
            work_queue_.push_back(WorkItem{
                .fd = conn.fd,
                .tokens = std::move(tokens)
            });
        }
        work_cv_.notify_one();

        if (is_quit) {
            break;
        }
    }
}

std::string Server::executeCommand(const std::vector<std::string>& tokens) {
    if (tokens.empty()) return respErr("empty command");

    std::string cmd = toUpper(tokens[0]);

    if (cmd == "PING")    return "+PONG\r\n";
    if (cmd == "SET")     return cmdSet(tokens);
    if (cmd == "GET")     return cmdGet(tokens);
    if (cmd == "DEL")     return cmdDel(tokens);
    if (cmd == "EXISTS")  return cmdExists(tokens);
    if (cmd == "KEYS")    return cmdKeys(tokens);
    if (cmd == "STATS")   return cmdStats(tokens);
    if (cmd == "COMPACT") return cmdCompact(tokens);
    if (cmd == "QUIT")    return "+BYE\r\n";

    return respErr("unknown command '" + tokens[0] + "'");
}

// ── Command Handlers ───────────────────────────────────────────────────────

std::string Server::cmdSet(const std::vector<std::string>& t) {
    if (t.size() < 3)
        return respErr("SET requires key and value");
    // Value can contain spaces — join remaining tokens
    std::string value;
    for (size_t i = 2; i < t.size(); ++i) {
        if (i > 2) value += ' ';
        value += t[i];
    }
    try {
        engine_.put(t[1], value);
        return respOK();
    } catch (const std::exception& e) {
        return respErr(e.what());
    }
}

std::string Server::cmdGet(const std::vector<std::string>& t) {
    if (t.size() < 2) return respErr("GET requires key");
    try {
        auto val = engine_.get(t[1]);
        if (!val) return respNil();
        return respBulk(*val);
    } catch (const std::exception& e) {
        return respErr(e.what());
    }
}

std::string Server::cmdDel(const std::vector<std::string>& t) {
    if (t.size() < 2) return respErr("DEL requires key");
    try {
        bool removed = engine_.del(t[1]);
        return respInt(removed ? 1 : 0);
    } catch (const std::exception& e) {
        return respErr(e.what());
    }
}

std::string Server::cmdExists(const std::vector<std::string>& t) {
    if (t.size() < 2) return respErr("EXISTS requires key");
    return respInt(engine_.exists(t[1]) ? 1 : 0);
}

std::string Server::cmdKeys(const std::vector<std::string>&) {
    auto ks = engine_.keys();
    return respArray(ks);
}

std::string Server::cmdStats(const std::vector<std::string>&) {
    auto es = engine_.stats();
    std::ostringstream oss;
    oss << "# FlashDB Stats\r\n"
        << "total_keys:"       << es.total_keys       << "\r\n"
        << "log_file_bytes:"   << es.log_file_bytes   << "\r\n"
        << "total_reads:"      << es.total_reads      << "\r\n"
        << "total_writes:"     << es.total_writes      << "\r\n"
        << "total_deletes:"    << es.total_deletes    << "\r\n"
        << "compactions:"      << es.compactions      << "\r\n"
        << "connections_total:"    << stats_.connections_total.load()    << "\r\n"
        << "connections_active:"   << stats_.connections_active.load()   << "\r\n"
        << "commands_processed:"   << stats_.commands_processed.load()   << "\r\n"
        << "bytes_read:"           << stats_.bytes_read.load()           << "\r\n"
        << "bytes_written:"        << stats_.bytes_written.load()        << "\r\n";
    std::string s = oss.str();
    return respBulk(s);
}

std::string Server::cmdCompact(const std::vector<std::string>&) {
    if (!compactor_) return respErr("no compactor attached");
    bool ok = compactor_->compact_now();
    return ok ? respOK() : respErr("compaction already in progress");
}

// ── RESP helpers ───────────────────────────────────────────────────────────

std::string Server::respOK()  { return "+OK\r\n"; }
std::string Server::respNil() { return "$-1\r\n"; }

std::string Server::respErr(std::string_view msg) {
    return std::string("-ERR ") + std::string(msg) + "\r\n";
}

std::string Server::respBulk(std::string_view s) {
    return "$" + std::to_string(s.size()) + "\r\n" + std::string(s) + "\r\n";
}

std::string Server::respInt(int64_t n) {
    return ":" + std::to_string(n) + "\r\n";
}

std::string Server::respArray(const std::vector<std::string>& items) {
    std::string out = "*" + std::to_string(items.size()) + "\r\n";
    for (const auto& item : items)
        out += respBulk(item);
    return out;
}

// ── Event Loop ─────────────────────────────────────────────────────────────

void Server::eventLoop() {
#ifdef USE_KQUEUE
    const int MAX_EVENTS = 512;
    struct kevent events[MAX_EVENTS];
    struct timespec timeout;
    timeout.tv_sec  = 0;
    timeout.tv_nsec = 100'000'000; // 100ms

    while (running_) {
        int n = ::kevent(event_fd_, nullptr, 0, events, MAX_EVENTS, &timeout);
        if (n < 0) {
            if (errno == EINTR) continue;
            std::cerr << "[Server] kevent error: " << ::strerror(errno) << "\n";
            break;
        }
        for (int i = 0; i < n; ++i) {
            int fd = static_cast<int>(events[i].ident);
            if (events[i].flags & EV_ERROR) {
                closeConnection(fd);
                continue;
            }
            if (fd == listen_fd_) {
                acceptNewConnection(listen_fd_);
            } else if (events[i].filter == EVFILT_READ) {
                handleRead(fd);
            } else if (events[i].filter == EVFILT_WRITE) {
                handleWrite(fd);
            }
        }
    }
#else // Linux epoll
    const int MAX_EVENTS = 512;
    struct epoll_event events[MAX_EVENTS];

    while (running_) {
        int n = ::epoll_wait(event_fd_, events, MAX_EVENTS, 100 /*ms*/);
        if (n < 0) {
            if (errno == EINTR) continue;
            std::cerr << "[Server] epoll_wait error: " << ::strerror(errno) << "\n";
            break;
        }
        for (int i = 0; i < n; ++i) {
            int fd = events[i].data.fd;
            uint32_t ev = events[i].events;

            if (ev & (EPOLLERR | EPOLLHUP)) {
                closeConnection(fd);
                continue;
            }
            if (fd == listen_fd_) {
                acceptNewConnection(listen_fd_);
            } else {
                if (ev & EPOLLIN)  handleRead(fd);
                if (ev & EPOLLOUT) handleWrite(fd);
            }
        }
    }
#endif
}

void Server::workerLoop() {
    while (running_) {
        std::unique_lock<std::mutex> lock(work_mutex_);
        work_cv_.wait_for(lock, std::chrono::milliseconds(100), [this]{
            return !running_ || !work_queue_.empty();
        });
        // Drain queue
        while (!work_queue_.empty()) {
            WorkItem item = std::move(work_queue_.front());
            work_queue_.pop_front();
            lock.unlock();

            std::string response = executeCommand(item.tokens);
            bool is_quit = (!item.tokens.empty() && toUpper(item.tokens[0]) == "QUIT");

            {
                std::lock_guard<std::mutex> cl(conn_mutex_);
                auto it = conns_.find(item.fd);
                if (it != conns_.end()) {
                    it->second->write_buf += response;
                    if (is_quit) {
                        it->second->closing = true;
                    }
                    modifyFd(event_fd_, item.fd, true);
                }
            }

            ++stats_.commands_processed;

            lock.lock();
        }
    }
}

// ── Lifecycle ──────────────────────────────────────────────────────────────

void Server::start() {
    running_ = true;
    bindAndListen();

    event_fd_ = createEventFd();
    registerFd(event_fd_, listen_fd_, false);

    // Start worker pool
    for (size_t i = 0; i < cfg_.worker_threads; ++i)
        workers_.emplace_back(&Server::workerLoop, this);

    std::cout << "[Server] Worker threads: " << cfg_.worker_threads << "\n";

    // Run event loop in this thread (blocking)
    eventLoop();
}

void Server::stop() {
    if (!running_.exchange(false)) return;

    // Wake workers
    work_cv_.notify_all();
    for (auto& t : workers_)
        if (t.joinable()) t.join();
    workers_.clear();

    // Close all connections
    {
        std::lock_guard<std::mutex> lock(conn_mutex_);
        for (auto& [fd, _] : conns_) ::close(fd);
        conns_.clear();
    }

    if (event_fd_ >= 0) { ::close(event_fd_); event_fd_ = -1; }
    if (listen_fd_ >= 0) { ::close(listen_fd_); listen_fd_ = -1; }

    std::cout << "[Server] Stopped.\n";
}

} // namespace flashdb
