#include "deserializator.h"
#include "../codec/codec_int.h"
#include "../codec/codec_string.h"
#include <iostream>
#include <fstream>

Batch deserializatorBatch(std::ifstream& in, const std::string& filepath) {
    uint32_t read_batch_magic;
    uint32_t batch_num_rows;
    uint32_t int_len;
    uint32_t string_len;

    in.read((char *)&read_batch_magic, sizeof(read_batch_magic));
    if(read_batch_magic != batch_magic){
        std::cerr << "Invalid batch_magic";
        return Batch();
    }
    in.read((char *)&batch_num_rows, sizeof(batch_num_rows));
    in.read((char *)&int_len, sizeof(int_len));
    in.read((char *)&string_len, sizeof(string_len));

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
    uint32_t batches_len;
    in.read((char *)&batches_len, sizeof(batches_len));

    for (uint32_t i = 0; i < batches_len; i++) {
        batches.push_back(deserializatorBatch(in, filepath));
    }
    return batches;
}
