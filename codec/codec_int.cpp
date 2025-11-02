#include "codec_int.h"
#include <algorithm> 

struct EncodeIntColumn {
    std::string name;
    std::vector<uint8_t> compressed_data;
    uint64_t delta_base;
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

void decodeIntColumn(std::ifstream& in, std::vector<IntColumn>& columns) {
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

    columns.push_back(std::move(decodeSingleIntColumn(column)));
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

void encodeIntColumns(std::ofstream& out, std::vector<IntColumn>& columns){
    for(auto intColumn : columns) {
        encodeMetaDataInt(out, intColumn);
    }
}

void decodeIntColumns(std::ifstream& in, std::vector<IntColumn>& columns, uint32_t length) {
    for (uint32_t j = 0; j < length; j++) {
        decodeIntColumn(in, columns);
    }
}