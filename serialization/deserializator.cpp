#include "deserializator.h"
#include "../codec/codec_int.h"
#include "../codec/codec_string.h"
#include <iostream>
#include <fstream>
#include <algorithm>

Batch deserializatorBatch(std::ifstream& in, const std::string& filepath) {
    uint32_t read_batch_magic;
    uint32_t batch_num_rows;
    uint32_t int_len;
    uint32_t string_len;
    in.read(reinterpret_cast<char*>(&read_batch_magic), sizeof(read_batch_magic));
    if (!in) return Batch();
    if(read_batch_magic != batch_magic){
        std::cerr << "Invalid batch_magic";
        return Batch();
    }
    in.read(reinterpret_cast<char*>(&batch_num_rows), sizeof(batch_num_rows));
    in.read(reinterpret_cast<char*>(&int_len), sizeof(int_len));
    in.read(reinterpret_cast<char*>(&string_len), sizeof(string_len));

    Batch batch;
    batch.num_rows = batch_num_rows;
    decodeIntColumns(in, batch.intColumns, int_len);
    decodeStringColumns(in, batch.stringColumns, string_len);
    return batch;
}

std::vector<Batch> deserializator(const std::string& filepath) {
    std::vector<Batch> batches;
    std::ifstream in(filepath, std::ios::binary);
    if (!in) {
        std::cerr << "deserializator: cannot open file " << filepath << "\n";
        return {};
    }
    uint32_t read_file_magic;
    in.read((char *)&read_file_magic, sizeof(read_file_magic));
    if(read_file_magic != file_magic){
        std::cerr << "Invalid file_magic";
        return {};
    }
    while (true) {
        uint32_t token;
        if (!in.read(reinterpret_cast<char*>(&token), sizeof(token))) break;
        if (token == batch_magic) {
            uint32_t batch_num_rows = 0;
            uint32_t int_len = 0;
            uint32_t string_len = 0;
            if (!in.read(reinterpret_cast<char*>(&batch_num_rows), sizeof(batch_num_rows))) break;
            if (!in.read(reinterpret_cast<char*>(&int_len), sizeof(int_len))) break;
            if (!in.read(reinterpret_cast<char*>(&string_len), sizeof(string_len))) break;

            Batch b;
            b.num_rows = batch_num_rows;
            decodeIntColumns(in, b.intColumns, int_len);
            decodeStringColumns(in, b.stringColumns, string_len);
            if (b.intColumns.empty() && b.stringColumns.empty() && b.num_rows == 0) break;
            batches.push_back(std::move(b));
        } else {
            break;
        }
    }
    return batches;
}

const std::unordered_map<std::string, ColumnInfo> createMap(std::ifstream &in) {
    std::unordered_map<std::string, ColumnInfo> map;
    if (!in) return map;
    in.clear();
    in.seekg(0, std::ios::end);
    std::streamoff file_size = in.tellg();
    const std::streamoff min_entry_footer = static_cast<std::streamoff>(sizeof(uint64_t) + sizeof(uint8_t) + sizeof(uint16_t));
    if (file_size < min_entry_footer) return map;

    if (file_size >= static_cast<std::streamoff>(sizeof(uint32_t) + sizeof(uint64_t))) {
        uint64_t index_start = 0;
        in.seekg(file_size - static_cast<std::streamoff>(sizeof(uint64_t)), std::ios::beg);
        if (in.read(reinterpret_cast<char*>(&index_start), sizeof(index_start))) {
            uint32_t n = 0;
            in.seekg(file_size - static_cast<std::streamoff>(sizeof(uint64_t) + sizeof(uint32_t)), std::ios::beg);
            if (in.read(reinterpret_cast<char*>(&n), sizeof(n))) {
                if (index_start <= static_cast<uint64_t>(file_size) && index_start + static_cast<uint64_t>(min_entry_footer) <= static_cast<uint64_t>(file_size) - (sizeof(uint64_t) + sizeof(uint32_t))) {
                    in.seekg(static_cast<std::streamoff>(index_start), std::ios::beg);
                    bool ok = true;
                    for (uint32_t i = 0; i < n; ++i) {
                        uint16_t name_len = 0;
                        if (!in.read(reinterpret_cast<char*>(&name_len), sizeof(name_len))) { ok = false; break; }
                        std::string name;
                        if (name_len > 0) {
                            name.resize(name_len);
                            if (!in.read(&name[0], name_len)) { ok = false; break; }
                        }
                        uint8_t kind = 0;
                        if (!in.read(reinterpret_cast<char*>(&kind), sizeof(kind))) { ok = false; break; }
                        uint64_t offset = 0;
                        if (!in.read(reinterpret_cast<char*>(&offset), sizeof(offset))) { ok = false; break; }
                        map.emplace(std::move(name), ColumnInfo{offset, kind});
                    }
                    if (ok) {
                        return map;
                    }
                    map.clear();
                } 
            }
        }
    }

    std::streamoff cur = file_size;
    std::cout << "createMap: start backward parsing at file_size=" << file_size << "\n";
    while (true) {
        if (cur < min_entry_footer) {
            break;
        }

        cur -= static_cast<std::streamoff>(sizeof(uint64_t));
        in.seekg(cur, std::ios::beg);
        uint64_t offset = 0;
        if (!in.read(reinterpret_cast<char*>(&offset), sizeof(offset))) {
            break;
        }

        cur -= static_cast<std::streamoff>(sizeof(uint8_t));
        in.seekg(cur, std::ios::beg);
        uint8_t kind = 0;
        if (!in.read(reinterpret_cast<char*>(&kind), sizeof(kind))) {
            break;
        }

        cur -= static_cast<std::streamoff>(sizeof(uint16_t));
        in.seekg(cur, std::ios::beg);
        uint16_t name_len = 0;
        if (!in.read(reinterpret_cast<char*>(&name_len), sizeof(name_len))) {
            break;
        }

        if (cur < static_cast<std::streamoff>(name_len)) {
            break;
        }
        cur -= static_cast<std::streamoff>(name_len);
        in.seekg(cur, std::ios::beg);
        std::string name;
        if (name_len > 0) {
            name.resize(name_len);
            if (!in.read(&name[0], name_len)) {
                break;
            }
        }

        map.emplace(std::move(name), ColumnInfo{offset, kind});
    }

    return map;

}
void showMap(std::unordered_map<std::string, ColumnInfo> &map) {
    if (map.empty()) {
        std::cerr << "showMap: map is empty\n";
        return;
    }
    std::vector<std::pair<std::string, ColumnInfo>> items;
    items.reserve(map.size());
    for (auto &kv : map) items.emplace_back(kv.first, kv.second);
    std::sort(items.begin(), items.end(), [](auto &a, auto &b){ return a.first < b.first; });

    std::cerr << "Index entries (name -> (offset, kind)):\n";
    for (auto &it : items) {
        const std::string &name = it.first;
        uint64_t offset = it.second.first;
        uint8_t kind = it.second.second;
        const char *kind_s = (kind == STRING) ? "STRING" : ((kind == INTEGER) ? "INTEGER" : "UNKNOWN");
        std::cerr << "  '" << name << "' -> offset=" << offset << ", kind=" << kind_s << "\n";
    }
}

std::vector<Batch> readColumn(const std::string& filepath, std::string column){
    std::vector<Batch> batches;
    std::ifstream in(filepath, std::ios::binary);
    if (!in) {
        std::cerr << "deserializator: cannot open file " << filepath << "\n";
        return {};
    }
    uint32_t read_file_magic;
    in.read((char *)&read_file_magic, sizeof(read_file_magic));
    if(read_file_magic != file_magic){
        std::cerr << "Invalid file_magic";
        return {};
    }
    std::unordered_map<std::string, ColumnInfo> map = createMap(in);
    auto it = map.find(column);
    if (it == map.end()) {
        std::cerr << "readColumn: column not found: " << column << "\n";
        return {};
    }

    uint64_t col_offset = it->second.first;
    uint8_t col_kind = it->second.second;

    uint64_t cur_offset = col_offset;
    if (cur_offset == 0) return {};

    while (cur_offset != 0) {
        in.clear();
        in.seekg(static_cast<std::streamoff>(cur_offset), std::ios::beg);
        if (!in) {
            std::cerr << "readColumn: seek to offset " << cur_offset << " failed (RC09)\n";
            break;
        }

        if (col_kind == INTEGER) {
            auto p = decodeIntColumnBlock(in);
            uint64_t prev = p.first;
            IntColumn col = std::move(p.second);
            Batch b;
            b.num_rows = col.column.size();
            b.intColumns.push_back(std::move(col));
            batches.push_back(std::move(b));
            cur_offset = prev;
        } else if (col_kind == STRING) {
            auto p = decodeStringColumnBlock(in);
            uint64_t prev = p.first;
            StringColumn col = std::move(p.second);
            Batch b;
            b.num_rows = col.column.size();
            b.stringColumns.push_back(std::move(col));
            batches.push_back(std::move(b));
            cur_offset = prev;
        } else {
            break;
        }
    }

    std::reverse(batches.begin(), batches.end());
    return batches;
}