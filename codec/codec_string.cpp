#include "codec_string.h"
#include <zstd.h>

struct EncodeStringColumn {
    std::string name;
    std::vector<uint8_t> compressed_data;
    uint32_t uncompressed_size;  
    uint32_t compressed_size;
};

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

void decodeStringColumn(std::ifstream& in, std::vector<StringColumn>& columns) {
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

    columns.push_back(std::move(decodeSingleStringColumn(column)));
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

void encodeStringColumn(std::ofstream& out, StringColumn& column){
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

void decodeStringColumns(std::ifstream& in, std::vector<StringColumn>& columns, uint32_t length) {
    for (uint32_t j = 0; j < length; j++) {
        decodeStringColumn(in, columns);
    }
}

void encodeStringColumns(std::ofstream& out, std::vector<StringColumn>& columns) {
    for (auto column : columns) {
        encodeStringColumn(out, column);
    }
}