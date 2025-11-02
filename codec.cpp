#include "codec.h"
#include <zstd.h>
#include <algorithm> 

inline constexpr int compresion_level = 3;

struct EncodeIntColumn {
    std::string name;
    std::vector<uint8_t> compressed_data;
    uint64_t delta_base;
};

struct EncodeStringColumn {
    std::string name;
    std::vector<uint8_t> compressed_data;
    uint32_t uncompressed_size;  
    uint32_t compressed_size;
};

void variableLengthEncoding(std::vector<uint8_t> & compressed_data, std::vector<uint64_t>& column) {
    bool flag = true;
    for (auto value : column) {
        flag = true;
        while(flag) {
            uint8_t byte = value & 0x7F;  
            value >>= 7;
            if (byte == 0) {
                compressed_data.push_back(byte);
                flag = false;
            } else {
                byte |= 0x80;
                compressed_data.push_back(byte);
            }
        }
    }
}

void variableLengthDecoding(std::vector<uint8_t> & compressed_data, std::vector<uint64_t>& column) {
    uint64_t value = 0;
    uint8_t shift = 0;

    for (uint8_t chunk : compressed_data) {
        value |= (uint64_t)(chunk & 0x7F) << shift;
        if ((chunk & 0x80) == 0) {
            column.push_back(value);
            value = 0;
            shift = 0;
        } else {
            shift += 7;
        }
    }
}

void deltaDecoding(std::vector<uint64_t>& column, uint64_t base) {
    if (column.empty()) return;

    uint64_t size =column.size();
    for (uint64_t i = 1; i < size; i++) {
        column.at(i) += base;
    }
}

void deltaEncoding(std::vector<uint64_t>& column, uint64_t base) {
    if (column.empty()) return ;
    uint64_t size = column.size();

    for (uint64_t i = 1; i < size; i++){
        column.at(i) -= base;
    }
}


EncodeIntColumn encodeSingleIntColumn(IntColumn& column) {
    EncodeIntColumn out;
    out.name = column.name;
    if (column.column.empty()) {
        out.delta_base = 0;
        out.compressed_data.clear();
        return out;
    }
    auto min_it = std::min_element(column.column.begin(), column.column.end());
    out.delta_base = *min_it;
    deltaEncoding(column.column, out.delta_base);
    variableLengthEncoding(out.compressed_data, column.column);
    return out;
}

IntColumn decodeSingleIntColumn(EncodeIntColumn& column) {
    IntColumn out;
    out.name = column.name;
    variableLengthDecoding(column.compressed_data, out.column);
    deltaDecoding(out.column, column.delta_base);
    return out;
}

EncodeStringColumn* encodeSingleStringColumn(StringColumn& column) {
    auto* out = new EncodeStringColumn();

    size_t total_size = 0;
    for (const auto &s : column.column) {
        total_size += s.size() + 1; 
    }

    std::string blob;
    blob.reserve(total_size);
    for (const auto &val : column.column) {
        blob.append(val);
        blob.push_back('\0');
    }

    out->name = std::move(column.name);

    size_t uncompressed_size = blob.size();
    out->uncompressed_size = static_cast<uint32_t>(uncompressed_size);

    size_t bound = ZSTD_compressBound(uncompressed_size);
    out->compressed_data.resize(bound);

    size_t compressed_size = ZSTD_compress(
        out->compressed_data.data(),
        out->compressed_data.size(),
        blob.data(),
        uncompressed_size,
        compresion_level
    );

    if (ZSTD_isError(compressed_size)) {
        std::cerr << "ZSTD compression error: " << ZSTD_getErrorName(compressed_size) << "\n";
        delete out;
        return nullptr;
    }

    out->compressed_data.resize(compressed_size);
    out->compressed_size = static_cast<uint32_t>(compressed_size);

    return out;
}

void encodeMetaDataString(std::ofstream& out, StringColumn& column){
    EncodeStringColumn* col = encodeSingleStringColumn(column);
    uint32_t len = (*col).name.size();
    uint32_t uncompressed_size = (*col).uncompressed_size;
    uint32_t compressed_size = (*col).compressed_size;

    out.write((char*)&len, sizeof(len));
    out.write((char*)(*col).name.data(), len);

    out.write((char*)&uncompressed_size, sizeof(uncompressed_size));
    out.write((char*)&compressed_size , sizeof(compressed_size));

    out.write((char*)(*col).compressed_data.data(), compressed_size);
}

void encodeMetaDataInt(std::ofstream& out, IntColumn& column){
    EncodeIntColumn col = encodeSingleIntColumn(column);
    uint32_t len = col.name.size();
    uint32_t compressed_bits_length = col.compressed_data.size();
    uint64_t delta_base = col.delta_base;

    out.write((char*)&len, sizeof(len));
    out.write((char*)col.name.data(), len);

    out.write((char*)&delta_base, sizeof(delta_base));

    out.write((char*)&compressed_bits_length, sizeof(compressed_bits_length));
    if (compressed_bits_length > 0) {
        out.write((char*)(col.compressed_data.data()), compressed_bits_length);
    }
}

StringColumn decodeSingleStringColumn(EncodeStringColumn& column) {

    StringColumn out;
    out.name = std::move(column.name);

    std::string decompressed(column.uncompressed_size, '\0');
    ZSTD_decompress(
        decompressed.data(),
        column.uncompressed_size,
        column.compressed_data.data(),
        column.compressed_size
    );

    size_t start = 0;
    for (size_t i = 0; i < decompressed.size(); ++i) {
        if (decompressed[i] == '\0') {
            out.column.emplace_back(decompressed.substr(start, i - start));
            start = i + 1;
        }
    }
    return out;
}

void decodeStringColumn(std::vector<StringColumn>* columns, std::ifstream& in) {
    EncodeStringColumn column;

    std::string name;
    uint32_t name_len;
    uint32_t uncompressed_size;
    uint32_t compressed_size;

    in.read(reinterpret_cast<char*>(&name_len), sizeof(name_len));
    name.resize(name_len);
    if (name_len > 0) {
        in.read(&name[0], name_len);
    }
    column.name = std::move(name);

    in.read(reinterpret_cast<char*>(&uncompressed_size), sizeof(uncompressed_size));
    in.read(reinterpret_cast<char*>(&compressed_size), sizeof(compressed_size));
    column.uncompressed_size = uncompressed_size;
    column.compressed_size = compressed_size;
    column.compressed_data.resize(static_cast<size_t>(compressed_size));
    if (compressed_size > 0) {
        in.read(reinterpret_cast<char*>(column.compressed_data.data()), compressed_size);
    }

    columns->push_back(std::move(decodeSingleStringColumn(column)));
}


void decodeIntColumn(std::vector<IntColumn>* columns, std::ifstream& in) {
    EncodeIntColumn column;

    std::string name;
    uint32_t name_len;
    uint32_t compressed_bits_length;
    uint64_t delta_base;

    in.read(reinterpret_cast<char*>(&name_len), sizeof(name_len));
    name.resize(name_len);
    if (name_len > 0) {
        in.read(&name[0], name_len);
    }
    column.name = std::move(name);

    in.read(reinterpret_cast<char*>(&delta_base), sizeof(delta_base));
    column.delta_base = delta_base;

    in.read(reinterpret_cast<char*>(&compressed_bits_length), sizeof(compressed_bits_length));
    column.compressed_data.resize(static_cast<size_t>(compressed_bits_length));
    if (compressed_bits_length > 0) {
        in.read(reinterpret_cast<char*>(column.compressed_data.data()), compressed_bits_length);
    }

    columns->push_back(std::move(decodeSingleIntColumn(column)));
}