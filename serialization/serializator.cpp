#include "serializator.h"
#include "../codec/codec_int.h"
#include "../codec/codec_string.h"
#include <iostream>
#include <fstream>
#include <unordered_map>
#include <vector>
#include <cstdio>
#include <algorithm>

using namespace std;


static std::string nextFilePath(const std::string& folderPath, uint32_t counter){
    char buf[512];
    std::snprintf(buf, sizeof(buf), "%s.part%03u", folderPath.c_str(), counter);
    return std::string(buf);
}

std::unordered_map<std::string, ColumnInfo> initMap(Batch batch) {
    std::unordered_map<std::string, ColumnInfo> m;
    for (const auto &ic : batch.intColumns) {
        m.emplace(ic.name, ColumnInfo{0, INTEGER});
    }
    for (const auto &sc : batch.stringColumns) {
        m.emplace(sc.name, ColumnInfo{0, STRING});
    }
    return m;
}
static std::ofstream startFile(const std::string& filepath) {
    std::ofstream out(filepath, std::ios::binary);
    if(!out) {
        std::cerr << "serializator: cannot open file " << filepath << "\n";
        return std::ofstream();
    }
    out.write(reinterpret_cast<const char*>(&file_magic), sizeof(file_magic));
    return out;
}

static void saveMapAndBatches(const std::unordered_map<std::string, ColumnInfo> &map, std::ofstream &out, uint32_t batches_in_part) {
    uint64_t index_start = static_cast<uint64_t>(out.tellp());
    for (const auto &kv : map) {
        const std::string &name = kv.first;
        uint16_t name_len = static_cast<uint16_t>(name.size());
        out.write(reinterpret_cast<const char*>(&name_len), sizeof(name_len));
        if (name_len) out.write(name.data(), name_len);
        uint8_t kind = kv.second.second; 
        out.write(reinterpret_cast<const char*>(&kind), sizeof(kind));
        uint64_t offset = kv.second.first;
        out.write(reinterpret_cast<const char*>(&offset), sizeof(offset));
    }
    uint32_t n = static_cast<uint32_t>(map.size());
    out.write(reinterpret_cast<const char*>(&n), sizeof(n));
    out.write(reinterpret_cast<const char*>(&index_start), sizeof(index_start));
}

static void clearMap(std::unordered_map<std::string, ColumnInfo> &map) {
    for (auto &kv : map) {
        kv.second.first = 0;
    }
}

void serializator(std::vector<Batch> &batches, const std::string& folderPath, uint64_t PART_LIMIT) {
    std::unordered_map<std::string, ColumnInfo> last_offset;
    if (!batches.empty()) last_offset = initMap(batches[0]);
    else last_offset = initMap(Batch());

    uint32_t file_counter = 0;
    std::ofstream out = startFile(nextFilePath(folderPath, file_counter));
    if (!out) return;
    uint32_t batches_in_part = 0;
    uint64_t file_pos = sizeof(file_magic);
    for (uint32_t batch_idx = 0; batch_idx < batches.size(); ++batch_idx) {
        Batch &batch = batches[batch_idx];
        out.write(reinterpret_cast<const char*>(&batch_magic), sizeof(batch_magic));
        file_pos += sizeof(batch_magic);

        uint32_t batch_num_rows32 = static_cast<uint32_t>(batch.num_rows);
        out.write(reinterpret_cast<const char*>(&batch_num_rows32), sizeof(batch_num_rows32));
        file_pos += sizeof(batch_num_rows32);

        uint32_t int_len = static_cast<uint32_t>(batch.intColumns.size());
        uint32_t string_len = static_cast<uint32_t>(batch.stringColumns.size());
        out.write(reinterpret_cast<const char*>(&int_len), sizeof(int_len));
        out.write(reinterpret_cast<const char*>(&string_len), sizeof(string_len));
        file_pos += sizeof(int_len) + sizeof(string_len);

        for (auto &intColumn : batch.intColumns) {
            uint64_t prev = 0;
            auto it = last_offset.find(intColumn.name);
            if (it != last_offset.end()) prev = it->second.first;

            uint64_t cur_offset = file_pos;
            out.write(reinterpret_cast<const char*>(&prev), sizeof(prev));
            file_pos += sizeof(prev);

            uint64_t written = encodeMetaDataInt(out, intColumn);
            file_pos += written;

            last_offset[intColumn.name] = ColumnInfo{cur_offset, INTEGER};
        }

        for (auto &stringColumn : batch.stringColumns) {
            uint64_t prev = 0;
            auto it = last_offset.find(stringColumn.name);
            if (it != last_offset.end()) prev = it->second.first;

            uint64_t cur_offset = file_pos;
            out.write(reinterpret_cast<const char*>(&prev), sizeof(prev));
            file_pos += sizeof(prev);

            uint64_t written = encodeStringColumn(out, stringColumn);
            file_pos += written;

            last_offset[stringColumn.name] = ColumnInfo{cur_offset, STRING};
        }
        out.flush();
        uint64_t cur_size = file_pos;
        ++batches_in_part;
        if (cur_size > PART_LIMIT) {
            
            saveMapAndBatches(last_offset, out, batches_in_part);
            out.close();

            ++file_counter;
            clearMap(last_offset);
            out = startFile(nextFilePath(folderPath, file_counter));
            if (!out) return;
            batches_in_part = 0;
            file_pos = sizeof(file_magic);
        }
    }
    if (out) {
        saveMapAndBatches(last_offset, out, batches_in_part);
        out.close();
    }
}