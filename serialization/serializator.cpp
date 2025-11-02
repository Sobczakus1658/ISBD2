#include "serializator.h"
#include "../codec/codec_int.h"
#include "../codec/codec_string.h"
#include <iostream>
#include <fstream>

void serializator(std::vector<Batch> &batches, const std::string& filepath) {
    std::ofstream out(filepath, std::ios::binary);
    uint32_t batches_len = batches.size();

    if(!out) {
        std::cerr << "serializator: cannot open file " << filepath << "\n";
        return;
    }

    out.write((char*)&file_magic, sizeof(file_magic));
    out.write((char*)&batches_len, sizeof(batches_len));

    for (auto &batch : batches) {
        uint32_t int_len = batch.intColumns.size();
        uint32_t string_len = batch.stringColumns.size();

        out.write((char*)&batch_magic, sizeof(batch_magic));
        uint32_t batch_num_rows32 = static_cast<uint32_t>(batch.num_rows);
        out.write(reinterpret_cast<char*>(&batch_num_rows32), sizeof(batch_num_rows32));

        out.write((char*)&int_len, sizeof(int_len));
        out.write((char*)&string_len, sizeof(string_len));

        encodeIntColumns(out, batch.intColumns);
        encodeStringColumns(out, batch.stringColumns);
    }
}