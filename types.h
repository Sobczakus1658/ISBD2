#include <vector>
#pragma once

#include <vector>
#include <string>
#include <cstdint>
#include <cstddef>

inline constexpr std::size_t BATCH_SIZE = 8192;
inline constexpr std::size_t FILE_SIZE_MAX = 10000;
inline constexpr std::uint32_t file_magic = 0x21374201;
inline constexpr std::uint32_t batch_magic = 0x69696969;
static constexpr uint64_t PART_LIMIT = 3500ULL * 1024ULL * 1024ULL;
static constexpr uint8_t INTEGER = 0;
static constexpr uint8_t STRING  = 1;

using std::uint32_t;
using ColumnInfo = std::pair<uint64_t, uint8_t>;

struct IntColumn {
    std::string name;
    std::vector<std::uint64_t> column;
};

struct StringColumn {
    std::string name;
    std::vector<std::string> column;
};

struct Batch {
    std::vector<IntColumn> intColumns;
    std::vector<StringColumn> stringColumns;
    std::size_t num_rows;
};

