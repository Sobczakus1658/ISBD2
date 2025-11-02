#include <vector>
#pragma once

#include <vector>
#include <string>
#include <cstdint>
#include <cstddef>

inline constexpr std::size_t BATCH_SIZE = 8192;
inline constexpr std::uint32_t file_magic = 0x21374201;
inline constexpr std::uint32_t batch_magic = 0x69696969;

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