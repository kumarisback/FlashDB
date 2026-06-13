cmake_minimum_required(VERSION 3.20)
project(FlashDB VERSION 1.0.0 LANGUAGES CXX)

# ── C++17 Standard ─────────────────────────────────────────────────────────
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ── Build Types ────────────────────────────────────────────────────────────
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Build type" FORCE)
endif()

# ── Compiler Flags ─────────────────────────────────────────────────────────
set(COMMON_FLAGS "-Wall -Wextra -Wpedantic -Wno-unused-parameter")

set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native -DNDEBUG ${COMMON_FLAGS}")
set(CMAKE_CXX_FLAGS_DEBUG   "-O0 -g3 -fsanitize=address,undefined ${COMMON_FLAGS}")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g -DNDEBUG ${COMMON_FLAGS}")

# ── Include Directories ────────────────────────────────────────────────────
include_directories(${CMAKE_SOURCE_DIR}/include)

# ── Source Files ───────────────────────────────────────────────────────────
set(ENGINE_SOURCES
    src/FlashDB.cpp
    src/Compactor.cpp
    src/Server.cpp
)

# ── Main Executable ────────────────────────────────────────────────────────
add_executable(FlashDB
    ${ENGINE_SOURCES}
    main.cpp
)

# Link threading library
find_package(Threads REQUIRED)
target_link_libraries(FlashDB PRIVATE Threads::Threads)

# macOS-specific: link kqueue (built into libc, nothing extra needed)
# Linux-specific: epoll also built into glibc

# ── Benchmark Executable ───────────────────────────────────────────────────
add_executable(benchmark
    ${ENGINE_SOURCES}
    benchmarks/stress_test.cpp
)
target_link_libraries(benchmark PRIVATE Threads::Threads)

# ── Install ────────────────────────────────────────────────────────────────
install(TARGETS FlashDB DESTINATION bin)
install(DIRECTORY include/ DESTINATION include/flashdb)

# ── Summary ────────────────────────────────────────────────────────────────
message(STATUS "=================================================")
message(STATUS "  FlashDB — Bitcask Key-Value Engine")
message(STATUS "  Build type  : ${CMAKE_BUILD_TYPE}")
message(STATUS "  C++ standard: C++${CMAKE_CXX_STANDARD} (designated inits)")
message(STATUS "  Compiler    : ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "  Install to  : ${CMAKE_INSTALL_PREFIX}")
message(STATUS "=================================================")
